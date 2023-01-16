package io.github.jorgerojasdev.parallelkstream.internal;

import io.github.jorgerojasdev.parallelkstream.exception.DeserializationException;
import io.github.jorgerojasdev.parallelkstream.exception.ProducerException;
import io.github.jorgerojasdev.parallelkstream.exception.handler.ParallelKStreamExceptionHandler;
import io.github.jorgerojasdev.parallelkstream.exception.handler.action.ExceptionHandlerAction;
import io.github.jorgerojasdev.parallelkstream.internal.model.common.Record;
import io.github.jorgerojasdev.parallelkstream.internal.model.node.Node;
import io.github.jorgerojasdev.parallelkstream.internal.model.node.SourceNode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.security.SecureRandom;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Getter
@RequiredArgsConstructor
public class SubTopology<K, V> {

    private static final String START_NODE_ID = String.format("start-node-%s", new SecureRandom().nextInt());

    private final String subTopologyName;
    private final SourceNode<K, V> sourceNode;
    private final Map<String, Node<?, ?, ?, ?>> nodeMap = new HashMap<>();
    private final ParallelKStreamExceptionHandler exceptionHandler;
    private final Runnable onStopApplication;

    public void init() {
        sourceNode.init();
    }

    public void start() {
        sourceNode.start(pollContext -> {
            Record<K, V> recordKv = null;
            ExceptionHandlerAction exceptionHandlerAction = null;
            try {
                recordKv = Record.fromConsumerRecord(pollContext.getSingleConsumerRecord());
                process(recordKv);
            } catch (DeserializationException e) {
                exceptionHandlerAction = exceptionHandler.deserializeExceptionHandle(e, pollContext);
            } catch (ProducerException e) {
                exceptionHandlerAction = exceptionHandler.produceExceptionHandle(e, recordKv);
            } catch (Exception e) {
                exceptionHandlerAction = exceptionHandler.uncaughtExceptionHandle(e, recordKv);
            }

            if (exceptionHandlerAction == null) {
                return;
            }
            if (ExceptionHandlerAction.CONTINUE.equals(exceptionHandlerAction)) {
                return;
            }

            if (ExceptionHandlerAction.STOP_THREAD.equals(exceptionHandlerAction)) {
                log.warn("Exception detected, stoping thread");
                this.pause();
                return;
            }

            if (ExceptionHandlerAction.STOP_APPLICATION.equals(exceptionHandlerAction)) {
                log.warn("Exception detected, stoping thread");
                onStopApplication.run();
            }
        });
    }

    public void pause() {
        sourceNode.pause();
    }

    public void resume() {
        sourceNode.resume();
    }

    public void addNode(Node<?, ?, ?, ?> parentNode, Node<?, ?, ?, ?> node) {
        if (parentNode == null) {
            if (sourceNode != null) {
                sourceNode.addChildRef(node.getNodeName());
                nodeMap.put(node.getNodeName(), node);
                return;
            }
            nodeMap.put(START_NODE_ID, node);
            return;
        }
        parentNode.addChild(node);
        nodeMap.put(node.getNodeName(), node);
    }

    public void process(Record<?, ?> recordKv) {

        if (nodeMap.containsKey(START_NODE_ID)) {
            process(recordKv, START_NODE_ID);
            return;
        }

        sourceNode.getChildRefs().forEach(childRef -> process(recordKv, childRef));
    }

    public void process(Record<?, ?> recordKv, String nodeRef) {
        Node<?, ?, ?, ?> nextNode = nodeMap.get(nodeRef);

        if (nextNode != null) {
            processNode(nextNode, recordKv);
        }
    }

    private void processNode(Node<?, ?, ?, ?> node, Record<?, ?> recordKv) {

        if (node == null) {
            return;
        }

        List<? extends Record<?, ?>> nextRecordList = node.process(recordKv);

        if (nextRecordList == null || nextRecordList.isEmpty()) {
            return;
        }

        nextRecordList.forEach(nextRecord -> processNestedRecord(node, nextRecord));
    }

    private void processNestedRecord(Node<?, ?, ?, ?> node, Record<?, ?> nextRecord) {
        for (String childRef : node.toChildrenRefs(nextRecord)) {
            Node<?, ?, ?, ?> childNode = nodeMap.get(childRef);

            if (childNode == null) {
                continue;
            }

            processNode(childNode, nextRecord);
        }
    }
}

package io.github.jorgerojasdev.parallelkstream.internal;

import io.github.jorgerojasdev.parallelkstream.internal.model.common.Record;
import io.github.jorgerojasdev.parallelkstream.internal.model.node.Node;
import io.github.jorgerojasdev.parallelkstream.internal.model.node.SourceNode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Getter
@RequiredArgsConstructor
public class SubTopology<K, V> {

    private final String subTopologyName;
    private final SourceNode<K, V> sourceNode;
    private final Map<String, Node<?, ?, ?, ?>> nodeMap = new HashMap<>();

    public void addNode(Node<?, ?, ?, ?> parentNode, Node<?, ?, ?, ?> node) {
        if (parentNode == null) {
            nodeMap.put("0", node);
            return;
        }
        parentNode.addChild(node);
        nodeMap.put(node.getNodeName(), node);
    }

    public void process(Record<?, ?> recordKv) {
        Node<?, ?, ?, ?> nextNode = nodeMap.get("0");

        processNode(nextNode, recordKv);
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
        for (String childRef : node.getChildren()) {
            Node<?, ?, ?, ?> childNode = nodeMap.get(childRef);

            if (childNode == null) {
                continue;
            }

            processNode(childNode, nextRecord);
        }
    }
}

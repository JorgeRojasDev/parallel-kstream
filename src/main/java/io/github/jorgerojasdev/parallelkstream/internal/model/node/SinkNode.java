package io.github.jorgerojasdev.parallelkstream.internal.model.node;

import io.github.jorgerojasdev.parallelkstream.internal.model.common.KeyValue;
import io.github.jorgerojasdev.parallelkstream.internal.model.common.Record;
import io.github.jorgerojasdev.parallelkstream.utils.NodeUtils;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

public class SinkNode<K, V> extends Node<K, V, K, V> {

    public static final String DEFAULT_NAME = "PARALLEL-KSTREAM-SINK";

    public SinkNode(Collection<String> topics) {
        super(NodeUtils.defaultNodeName(DEFAULT_NAME), sinkFunction(topics));
    }

    public SinkNode(String nodeName, Collection<String> topics) {
        super(nodeName, sinkFunction(topics));
    }

    private static <K, V> Function<Record<K, V>, List<KeyValue<K, V>>> sinkFunction(Collection<String> topics) {
        return recordKv -> {
            topics.forEach(topic -> {
                //TODO
            });
            return Collections.emptyList();
        };
    }

}

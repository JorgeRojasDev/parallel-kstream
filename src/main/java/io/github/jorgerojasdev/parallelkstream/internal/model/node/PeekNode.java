package io.github.jorgerojasdev.parallelkstream.internal.model.node;

import io.github.jorgerojasdev.parallelkstream.internal.model.common.KeyValue;
import io.github.jorgerojasdev.parallelkstream.internal.model.common.Record;
import io.github.jorgerojasdev.parallelkstream.utils.NodeUtils;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

public class PeekNode<K, V> extends Node<K, V, K, V> {

    public static final String DEFAULT_NAME = "PARALLEL-KSTREAM-PEEK";

    public PeekNode(Consumer<Record<K, V>> peekConsumer) {
        super(NodeUtils.defaultNodeName(DEFAULT_NAME), peekFunction(peekConsumer));
    }

    public PeekNode(String nodeName, Consumer<Record<K, V>> peekConsumer) {
        super(nodeName, peekFunction(peekConsumer));
    }

    private static <K, V> Function<Record<K, V>, List<KeyValue<K, V>>> peekFunction(Consumer<Record<K, V>> peekConsumer) {
        return recordKv -> {
            peekConsumer.accept(recordKv);
            return List.of(KeyValue.pair(recordKv.getKey(), recordKv.getValue()));
        };
    }

}

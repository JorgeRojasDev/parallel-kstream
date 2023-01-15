package io.github.jorgerojasdev.parallelkstream.internal.model.node;

import io.github.jorgerojasdev.parallelkstream.internal.model.common.KeyValue;
import io.github.jorgerojasdev.parallelkstream.internal.model.common.Record;
import io.github.jorgerojasdev.parallelkstream.utils.NodeUtils;

import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

public class ForEachNode<K, V> extends Node<K, V, K, V> {

    public static final String DEFAULT_NAME = "PARALLEL-KSTREAM-FOREACH";

    public ForEachNode(Consumer<Record<K, V>> peekConsumer) {
        super(NodeUtils.defaultNodeName(DEFAULT_NAME), forEachFunction(peekConsumer));
    }

    public ForEachNode(String nodeName, Consumer<Record<K, V>> peekConsumer) {
        super(nodeName, forEachFunction(peekConsumer));
    }

    private static <K, V> Function<Record<K, V>, List<KeyValue<K, V>>> forEachFunction(Consumer<Record<K, V>> forEachConsumer) {
        return recordKv -> {
            forEachConsumer.accept(recordKv);
            return Collections.emptyList();
        };
    }

}

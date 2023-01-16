package io.github.jorgerojasdev.parallelkstream.internal.model.node;

import io.github.jorgerojasdev.parallelkstream.internal.model.common.KeyValue;
import io.github.jorgerojasdev.parallelkstream.internal.model.common.Record;
import io.github.jorgerojasdev.parallelkstream.utils.NodeUtils;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

public class FilterNode<K, V> extends Node<K, V, K, V> {

    public static final String DEFAULT_NAME = "PARALLEL-KSTREAM-FILTER";

    public FilterNode(Predicate<Record<K, V>> predicate) {
        this(NodeUtils.defaultNodeName(DEFAULT_NAME), predicate);
    }

    public FilterNode(String nodeName, Predicate<Record<K, V>> predicate) {
        super(nodeName, filterFunction(predicate));
    }

    private static <K, V> Function<Record<K, V>, List<KeyValue<K, V>>> filterFunction(Predicate<Record<K, V>> predicate) {
        return recordKv -> {
            if (predicate.test(recordKv)) {
                return List.of(KeyValue.pair(recordKv.getKey(), recordKv.getValue()));
            }
            return Collections.emptyList();
        };
    }

}

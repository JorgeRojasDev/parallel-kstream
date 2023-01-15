package io.github.jorgerojasdev.parallelkstream.internal.model.node;

import io.github.jorgerojasdev.parallelkstream.internal.model.common.KeyValue;
import io.github.jorgerojasdev.parallelkstream.internal.model.common.Record;
import io.github.jorgerojasdev.parallelkstream.utils.NodeUtils;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

public class FilterNotNode<K, V> extends Node<K, V, K, V> {

    public static final String DEFAULT_NAME = "PARALLEL-KSTREAM-FILTER-NOT";

    public FilterNotNode(Predicate<Record<K, V>> predicate) {
        super(NodeUtils.defaultNodeName(DEFAULT_NAME), filterFunction(predicate));
    }

    public FilterNotNode(String nodeName, Predicate<Record<K, V>> predicate) {
        super(nodeName, filterFunction(predicate));
    }

    private static <K, V> Function<Record<K, V>, List<KeyValue<K, V>>> filterFunction(Predicate<Record<K, V>> predicate) {
        return recordKv -> {
            if (predicate.test(recordKv)) {
                return Collections.emptyList();
            }
            return List.of(KeyValue.pair(recordKv.getKey(), recordKv.getValue()));
        };
    }

}

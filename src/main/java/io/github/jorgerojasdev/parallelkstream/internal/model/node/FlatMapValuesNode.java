package io.github.jorgerojasdev.parallelkstream.internal.model.node;

import io.github.jorgerojasdev.parallelkstream.internal.model.common.KeyValue;
import io.github.jorgerojasdev.parallelkstream.internal.model.common.Record;
import io.github.jorgerojasdev.parallelkstream.utils.NodeUtils;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FlatMapValuesNode<K, V, NV> extends Node<K, V, K, NV> {

    public static final String DEFAULT_NAME = "P-KSTREAM-FLATMAPVALUES";

    public FlatMapValuesNode(Function<Record<K, V>, Stream<NV>> flatMapValuesFunction) {
        this(NodeUtils.defaultNodeName(DEFAULT_NAME), flatMapValuesFunction);
    }

    public FlatMapValuesNode(String nodeName, Function<Record<K, V>, Stream<NV>> flatMapValuesFunction) {
        super(nodeName, modifyFlatMapValuesFunction(flatMapValuesFunction));
    }

    private static <K, V, NV> Function<Record<K, V>, List<KeyValue<K, NV>>> modifyFlatMapValuesFunction(Function<Record<K, V>, Stream<NV>> flatMapValuesFunction) {
        return recordKv -> flatMapValuesFunction.apply(recordKv).map(value -> KeyValue.pair(recordKv.getKey(), value)).collect(Collectors.toList());
    }
}

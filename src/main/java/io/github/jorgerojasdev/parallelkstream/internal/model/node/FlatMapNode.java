package io.github.jorgerojasdev.parallelkstream.internal.model.node;

import io.github.jorgerojasdev.parallelkstream.internal.model.common.KeyValue;
import io.github.jorgerojasdev.parallelkstream.internal.model.common.Record;
import io.github.jorgerojasdev.parallelkstream.utils.NodeUtils;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FlatMapNode<K, V, NK, NV> extends Node<K, V, NK, NV> {

    public static final String DEFAULT_NAME = "P-KSTREAM-FLATMAP";

    public FlatMapNode(Function<Record<K, V>, Stream<KeyValue<NK, NV>>> flatMapFunction) {
        super(NodeUtils.defaultNodeName(DEFAULT_NAME), modifyFlatMapFunction(flatMapFunction));
    }

    public FlatMapNode(String nodeName, Function<Record<K, V>, Stream<KeyValue<NK, NV>>> flatMapFunction) {
        super(nodeName, modifyFlatMapFunction(flatMapFunction));
    }

    private static <K, V, NK, NV> Function<Record<K, V>, List<KeyValue<NK, NV>>> modifyFlatMapFunction(Function<Record<K, V>, Stream<KeyValue<NK, NV>>> flatMapFunction) {
        return recordKv -> flatMapFunction.apply(recordKv).collect(Collectors.toList());
    }
}

package io.github.jorgerojasdev.parallelkstream.internal.model.node;

import io.github.jorgerojasdev.parallelkstream.internal.model.common.Record;
import io.github.jorgerojasdev.parallelkstream.utils.NodeUtils;

import java.util.List;
import java.util.function.Function;

public class MapValuesNode<K, V, NV> extends Node<K, V, K, NV> {

    public static final String DEFAULT_NAME = "PARALLEL-KSTREAM-MAPVALUES-NODE";

    public MapValuesNode(Function<Record<K, V>, NV> mapValuesFunction) {
        super(NodeUtils.defaultNodeName(DEFAULT_NAME), mapValuesFunction(mapValuesFunction));
    }

    public MapValuesNode(String nodeName, Function<Record<K, V>, NV> mapValuesFunction) {
        super(nodeName, mapValuesFunction(mapValuesFunction));
    }

    private static <K, V, NV> Function<Record<K, V>, List<KeyValue<K, NV>>> mapValuesFunction(Function<Record<K, V>, NV> mapValuesFunction) {
        return recordKv -> List.of(KeyValue.pair(recordKv.getKey(), mapValuesFunction.apply(recordKv)));
    }
}

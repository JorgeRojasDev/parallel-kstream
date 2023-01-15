package io.github.jorgerojasdev.parallelkstream.internal.model.node;

import io.github.jorgerojasdev.parallelkstream.internal.model.common.KeyValue;
import io.github.jorgerojasdev.parallelkstream.internal.model.common.Record;
import io.github.jorgerojasdev.parallelkstream.utils.NodeUtils;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;

public class MapNode<K, V, NK, NV> extends Node<K, V, NK, NV> {

    public static final String DEFAULT_NAME = "PARALLEL-KSTREAM-MAP";

    public MapNode(Function<Record<K, V>, KeyValue<NK, NV>> mapFunction) {
        super(NodeUtils.defaultNodeName(DEFAULT_NAME), modifyMapFunction(mapFunction));
    }

    public MapNode(String nodeName, Function<Record<K, V>, KeyValue<NK, NV>> mapFunction) {
        super(nodeName, modifyMapFunction(mapFunction));
    }

    private static <K, V, NK, NV> Function<Record<K, V>, List<KeyValue<NK, NV>>> modifyMapFunction(Function<Record<K, V>, KeyValue<NK, NV>> mapFunction) {
        return recordKv -> {
            KeyValue<NK, NV> result = mapFunction.apply(recordKv);

            if (result == null) {
                return Collections.emptyList();
            }
            return List.of(result);
        };
    }
}

package io.github.jorgerojasdev.parallelkstream.internal.model.node;

import io.github.jorgerojasdev.parallelkstream.internal.model.common.KeyValue;
import io.github.jorgerojasdev.parallelkstream.utils.NodeUtils;

import java.util.List;

public class BranchNode<K, V> extends Node<K, V, K, V> {

    public static final String DEFAULT_NAME = "PARALLEL-KSTREAM-BRANCH";

    public BranchNode() {
        this(NodeUtils.defaultNodeName(DEFAULT_NAME));
    }

    public BranchNode(String nodeName) {
        super(nodeName, kvRecord -> List.of(KeyValue.pair(kvRecord.getKey(), kvRecord.getValue())));
    }

}

package io.github.jorgerojasdev.parallelkstream.internal.model.node;

import io.github.jorgerojasdev.parallelkstream.internal.model.common.BranchRef;
import io.github.jorgerojasdev.parallelkstream.internal.model.common.KeyValue;
import io.github.jorgerojasdev.parallelkstream.internal.model.common.Record;
import io.github.jorgerojasdev.parallelkstream.utils.NodeUtils;
import lombok.Getter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

@Getter
public class SplitNode<K, V> extends Node<K, V, K, V> {

    public static final String DEFAULT_NAME = "PARALLEL-KSTREAM-SPLIT";

    private final List<BranchRef<K, V>> branchRefs = new ArrayList<>();

    private BranchRef<K, V> defaultBranchRef;

    public SplitNode() {
        this(NodeUtils.defaultNodeName(DEFAULT_NAME));
    }

    public SplitNode(String nodeName) {
        super(nodeName, kvRecord -> List.of(KeyValue.pair(kvRecord.getKey(), kvRecord.getValue())));
    }

    public void addBranchRef(BranchRef<K, V> branchRef) {
        this.branchRefs.add(branchRef);
    }

    public void addDefaultBranchRef(BranchRef<K, V> defaultBranchRef) {
        this.defaultBranchRef = defaultBranchRef;
    }

    @Override
    public Set<String> toChildrenRefs(Record<?, ?> recordKv) {
        for (BranchRef<K, V> branchRef : branchRefs) {
            if (branchRef.getPredicate().test((Record<K, V>) recordKv)) {
                return Set.of(branchRef.getBranchNode().getNodeName());
            }
        }
        if (defaultBranchRef != null) {
            return Set.of(defaultBranchRef.getBranchNode().getNodeName());
        }
        return Collections.emptySet();
    }
}

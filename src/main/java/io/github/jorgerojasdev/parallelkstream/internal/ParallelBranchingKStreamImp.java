package io.github.jorgerojasdev.parallelkstream.internal;

import io.github.jorgerojasdev.parallelkstream.api.ParallelBranchedDefaultKStream;
import io.github.jorgerojasdev.parallelkstream.api.ParallelBranchedNoDefaultKStream;
import io.github.jorgerojasdev.parallelkstream.api.ParallelBranchingKStream;
import io.github.jorgerojasdev.parallelkstream.api.ParallelKStream;
import io.github.jorgerojasdev.parallelkstream.internal.model.common.Branch;
import io.github.jorgerojasdev.parallelkstream.internal.model.common.BranchRef;
import io.github.jorgerojasdev.parallelkstream.internal.model.node.BranchNode;
import io.github.jorgerojasdev.parallelkstream.internal.model.node.Node;
import io.github.jorgerojasdev.parallelkstream.internal.model.node.SplitNode;
import lombok.Builder;

import java.util.HashMap;
import java.util.Map;

@Builder
public class ParallelBranchingKStreamImp<K, V> implements ParallelBranchingKStream<K, V> {

    private final Topology topology;
    private final SubTopology<?, ?> currentSubTopology;
    private final Node<?, ?, ?, ?> fromNode;
    private final SplitNode<K, V> splitNode;
    private final Map<String, ParallelKStream<K, V>> parallelKStreamMap = new HashMap<>();
    private ParallelKStream<K, V> defaultParallelKStream;

    @Override
    public ParallelBranchingKStream<K, V> branch(Branch<K, V> branch) {
        BranchNode<K, V> branchNode = new BranchNode<>(branch.getBranchName());
        splitNode.addBranchRef(BranchRef.<K, V>builder().branchNode(branchNode).predicate(branch.getCondition()).build());
        return this;
    }

    @Override
    public ParallelBranchedDefaultKStream<K, V> defaultBranch() {
        BranchNode<K, V> defaultBranchNode = new BranchNode<>();
        splitNode.addDefaultBranchRef(BranchRef.<K, V>builder().branchNode(defaultBranchNode).predicate(recordKv -> true).build());
        finishBranched(defaultBranchNode);
        return ParallelBranchedDefaultKStream.<K, V>builder().parallelKStreamMap(parallelKStreamMap).defaultBranch(defaultParallelKStream).build();
    }

    @Override
    public ParallelBranchedNoDefaultKStream<K, V> noDefaultBranch() {
        finishBranched(null);
        return ParallelBranchedNoDefaultKStream.<K, V>builder().parallelKStreamMap(parallelKStreamMap).build();
    }

    private void finishBranched(BranchNode<K, V> defaultBranchNode) {
        currentSubTopology.addNode(fromNode, splitNode);
        if (defaultBranchNode != null) {
            currentSubTopology.addNode(splitNode, defaultBranchNode);
            this.defaultParallelKStream = new ParallelKStreamImp<>(topology, currentSubTopology, defaultBranchNode);
        }
        splitNode.getBranchRefs().forEach(branchRef -> {
            currentSubTopology.addNode(splitNode, branchRef.getBranchNode());
            this.parallelKStreamMap.put(branchRef.getBranchNode().getNodeName(), new ParallelKStreamImp<>(topology, currentSubTopology, branchRef.getBranchNode()));
        });
    }
}

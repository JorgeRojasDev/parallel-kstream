package io.github.jorgerojasdev.parallelkstream.api;

import io.github.jorgerojasdev.parallelkstream.internal.model.common.Branch;

public interface ParallelBranchingKStream<K, V> {

    ParallelBranchingKStream<K, V> branch(Branch<K, V> branch);

    ParallelBranchedDefaultKStream<K, V> defaultBranch();

    ParallelBranchedNoDefaultKStream<K, V> noDefaultBranch();
}

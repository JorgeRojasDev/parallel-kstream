package io.github.jorgerojasdev.parallelkstream.api;

import io.github.jorgerojasdev.parallelkstream.internal.model.common.Branch;

import java.util.Map;

public interface ParallelBranchedKStream<K, V> {

    ParallelKStream<K, V> branch(Branch<K, V> branch);

    Map<String, ParallelKStream<K, V>> defaultBranch();

    Map<String, ParallelKStream<K, V>> noDefaultBranch();
}

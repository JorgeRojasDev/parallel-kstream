package io.github.jorgerojasdev.parallelkstream.api;

import lombok.Builder;

import java.util.Map;

@Builder
public class ParallelBranchedDefaultKStream<K, V> {

    private final Map<String, ParallelKStream<K, V>> parallelKStreamMap;
    private final ParallelKStream<K, V> defaultBranch;

    public ParallelKStream<K, V> get(String branchName) {
        return parallelKStreamMap.get(branchName);
    }

    public ParallelKStream<K, V> defaultBranch() {
        return defaultBranch;
    }
}

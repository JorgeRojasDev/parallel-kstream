package io.github.jorgerojasdev.parallelkstream.api;

import lombok.Builder;

import java.util.Map;

@Builder
public class ParallelBranchedNoDefaultKStream<K, V> {

    private final Map<String, ParallelKStream<K, V>> parallelKStreamMap;

    public ParallelKStream<K, V> get(String branchName) {
        return parallelKStreamMap.get(branchName);
    }
}

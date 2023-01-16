package io.github.jorgerojasdev.parallelkstream.internal.model.common;

import io.github.jorgerojasdev.parallelkstream.internal.model.node.BranchNode;
import lombok.Builder;
import lombok.Getter;

import java.util.function.Predicate;

@Builder
@Getter
public class BranchRef<K, V> {

    private final BranchNode<K, V> branchNode;
    private final Predicate<Record<K, V>> predicate;
}

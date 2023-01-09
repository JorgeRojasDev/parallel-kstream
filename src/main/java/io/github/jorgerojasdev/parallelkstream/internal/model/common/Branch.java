package io.github.jorgerojasdev.parallelkstream.internal.model.common;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.function.Predicate;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Getter
public class Branch<K, V> {

    private final Predicate<Record<K, V>> condition;
    private final String branchName;

    public static <K, V> Branch<K, V> create(Predicate<Record<K, V>> condition, String branchName) {
        return new Branch<>(condition, branchName);
    }
}

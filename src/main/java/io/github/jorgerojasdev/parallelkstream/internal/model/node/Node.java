package io.github.jorgerojasdev.parallelkstream.internal.model.node;

import io.github.jorgerojasdev.parallelkstream.internal.model.common.KeyValue;
import io.github.jorgerojasdev.parallelkstream.internal.model.common.Record;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@RequiredArgsConstructor(access = AccessLevel.PROTECTED)
@Getter
public abstract class Node<K, V, NK, NV> {

    private final String nodeName;
    private final Set<String> children = new LinkedHashSet<>();
    private final Function<Record<K, V>, List<KeyValue<NK, NV>>> nodeFunction;

    public Set<String> toChildrenRefs(Record<?, ?> recordKv) {
        return children;
    }

    public void addChild(Node<?, ?, ?, ?> child) {
        children.add(child.getNodeName());
    }

    @SuppressWarnings("unchecked")
    public List<Record<NK, NV>> process(Record<?, ?> recordKv) {

        List<KeyValue<NK, NV>> keyValueList = nodeFunction.apply((Record<K, V>) recordKv);

        return keyValueList.stream().flatMap(keyValue -> {
            if (keyValue == null || (keyValue.getKey() == null && keyValue.getValue() == null)) {
                return Stream.empty();
            }
            return Stream.of(Record.fromAnotherRecord(recordKv, keyValue));
        }).collect(Collectors.toList());
    }
}

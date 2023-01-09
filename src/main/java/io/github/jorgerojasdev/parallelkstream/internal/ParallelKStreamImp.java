package io.github.jorgerojasdev.parallelkstream.internal;

import io.github.jorgerojasdev.parallelkstream.api.ParallelBranchedKStream;
import io.github.jorgerojasdev.parallelkstream.api.ParallelKStream;
import io.github.jorgerojasdev.parallelkstream.internal.model.common.Record;
import io.github.jorgerojasdev.parallelkstream.internal.model.node.*;
import io.github.jorgerojasdev.parallelkstream.internal.model.state.ProcessorContext;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.Collection;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

@RequiredArgsConstructor
@Getter
public class ParallelKStreamImp<K, V> implements ParallelKStream<K, V> {

    private final SubTopology subTopology;
    private final Node<?, ?, ?, ?> fromNode;

    @Override
    public ParallelKStream<K, V> filter(Predicate<Record<K, V>> predicate) {
        Node<?, ?, ?, ?> newNode = new FilterNode<>(predicate);
        subTopology.addNode(fromNode, newNode);
        return new ParallelKStreamImp<>(subTopology, newNode);
    }

    @Override
    public ParallelKStream<K, V> filterNot(Predicate<Record<K, V>> predicate) {
        Node<?, ?, ?, ?> newNode = new FilterNotNode<>(predicate);
        subTopology.addNode(fromNode, newNode);
        return new ParallelKStreamImp<>(subTopology, newNode);
    }

    @Override
    public <NK, NV> ParallelKStream<NK, NV> map(Function<Record<K, V>, KeyValue<NK, NV>> mapFunction) {
        Node<?, ?, ?, ?> newNode = new MapNode<>(mapFunction);
        subTopology.addNode(fromNode, newNode);
        return new ParallelKStreamImp<>(subTopology, newNode);
    }

    @Override
    public <NV> ParallelKStream<K, NV> mapValues(Function<Record<K, V>, NV> mapValueFunction) {
        Node<?, ?, ?, ?> newNode = new MapValuesNode<>(mapValueFunction);
        subTopology.addNode(fromNode, newNode);
        return new ParallelKStreamImp<>(subTopology, newNode);
    }

    @Override
    public <NK, NV> ParallelKStream<NK, NV> flatMap(Function<Record<K, V>, Stream<KeyValue<NK, NV>>> flatMapFunction) {
        Node<?, ?, ?, ?> newNode = new FlatMapNode<>(flatMapFunction);
        subTopology.addNode(fromNode, newNode);
        return new ParallelKStreamImp<>(subTopology, newNode);
    }

    @Override
    public <NV> ParallelKStream<K, NV> flatMapValues(Function<Record<K, V>, Stream<NV>> flatMapValuesFunction) {
        Node<?, ?, ?, ?> newNode = new FlatMapValuesNode<>(flatMapValuesFunction);
        subTopology.addNode(fromNode, newNode);
        return new ParallelKStreamImp<>(subTopology, newNode);
    }

    //TODO
    @Override
    public <NK, NV> ParallelKStream<NK, NV> transform(BiFunction<ProcessorContext, Record<K, V>, KeyValue<NK, NV>> transformFunction) {
        return null;
    }

    //TODO
    @Override
    public <NV> ParallelKStream<K, NV> transformValues(BiFunction<ProcessorContext, Record<K, V>, NV> transformValuesFunction) {
        return null;
    }

    @Override
    public ParallelKStream<K, V> peek(Consumer<Record<K, V>> peekConsumer) {
        Node<?, ?, ?, ?> newNode = new PeekNode<>(peekConsumer);
        subTopology.addNode(fromNode, newNode);
        return new ParallelKStreamImp<>(subTopology, newNode);
    }

    //TODO
    @Override
    public ParallelBranchedKStream<K, V> split() {
        return null;
    }

    @Override
    public void forEach(Consumer<Record<K, V>> forEachConsumer) {
        Node<?, ?, ?, ?> newNode = new ForEachNode<>(forEachConsumer);
        subTopology.addNode(fromNode, newNode);
    }

    //TODO
    @Override
    public void to(String topic) {
        //TODO
    }

    //TODO
    @Override
    public void to(Collection<String> topicCollection) {
        //TODO
    }

    @Override
    public SubTopology build() {
        return subTopology;
    }
}

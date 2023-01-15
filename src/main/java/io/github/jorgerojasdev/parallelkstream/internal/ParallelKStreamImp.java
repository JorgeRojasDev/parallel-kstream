package io.github.jorgerojasdev.parallelkstream.internal;

import io.github.jorgerojasdev.parallelkstream.api.ParallelBranchedKStream;
import io.github.jorgerojasdev.parallelkstream.api.ParallelKStream;
import io.github.jorgerojasdev.parallelkstream.internal.model.common.KeyValue;
import io.github.jorgerojasdev.parallelkstream.internal.model.common.Record;
import io.github.jorgerojasdev.parallelkstream.internal.model.node.*;
import io.github.jorgerojasdev.parallelkstream.internal.model.state.ProcessorContext;

import java.util.Collection;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

public class ParallelKStreamImp<K, V> implements ParallelKStream<K, V> {

    private final Topology topology;
    private final SubTopology<?, ?> currentSubTopology;
    private final Node<?, ?, ?, ?> fromNode;

    public ParallelKStreamImp(Topology topology, SubTopology<?, ?> subTopology, Node<?, ?, ?, ?> fromNode) {
        this.topology = topology;
        this.currentSubTopology = subTopology;
        this.fromNode = fromNode;
        this.topology.addSubtopology(this.currentSubTopology);
    }

    @Override
    public ParallelKStream<K, V> filter(Predicate<Record<K, V>> predicate) {
        Node<?, ?, ?, ?> newNode = new FilterNode<>(predicate);
        currentSubTopology.addNode(fromNode, newNode);
        return new ParallelKStreamImp<>(topology, currentSubTopology, newNode);
    }

    @Override
    public ParallelKStream<K, V> filterNot(Predicate<Record<K, V>> predicate) {
        Node<?, ?, ?, ?> newNode = new FilterNotNode<>(predicate);
        currentSubTopology.addNode(fromNode, newNode);
        return new ParallelKStreamImp<>(topology, currentSubTopology, newNode);
    }

    @Override
    public <NK, NV> ParallelKStream<NK, NV> map(Function<Record<K, V>, KeyValue<NK, NV>> mapFunction) {
        Node<?, ?, ?, ?> newNode = new MapNode<>(mapFunction);
        currentSubTopology.addNode(fromNode, newNode);
        return new ParallelKStreamImp<>(topology, currentSubTopology, newNode);
    }

    @Override
    public <NV> ParallelKStream<K, NV> mapValues(Function<Record<K, V>, NV> mapValueFunction) {
        Node<?, ?, ?, ?> newNode = new MapValuesNode<>(mapValueFunction);
        currentSubTopology.addNode(fromNode, newNode);
        return new ParallelKStreamImp<>(topology, currentSubTopology, newNode);
    }

    @Override
    public <NK, NV> ParallelKStream<NK, NV> flatMap(Function<Record<K, V>, Stream<KeyValue<NK, NV>>> flatMapFunction) {
        Node<?, ?, ?, ?> newNode = new FlatMapNode<>(flatMapFunction);
        currentSubTopology.addNode(fromNode, newNode);
        return new ParallelKStreamImp<>(topology, currentSubTopology, newNode);
    }

    @Override
    public <NV> ParallelKStream<K, NV> flatMapValues(Function<Record<K, V>, Stream<NV>> flatMapValuesFunction) {
        Node<?, ?, ?, ?> newNode = new FlatMapValuesNode<>(flatMapValuesFunction);
        currentSubTopology.addNode(fromNode, newNode);
        return new ParallelKStreamImp<>(topology, currentSubTopology, newNode);
    }

    //TODO
    @Override
    public <NK, NV> ParallelKStream<NK, NV> process(BiFunction<ProcessorContext, Record<K, V>, KeyValue<NK, NV>> transformFunction) {
        return null;
    }

    //TODO
    @Override
    public <NV> ParallelKStream<K, NV> processValues(BiFunction<ProcessorContext, Record<K, V>, NV> transformValuesFunction) {
        return null;
    }

    @Override
    public ParallelKStream<K, V> peek(Consumer<Record<K, V>> peekConsumer) {
        Node<?, ?, ?, ?> newNode = new PeekNode<>(peekConsumer);
        currentSubTopology.addNode(fromNode, newNode);
        return new ParallelKStreamImp<>(topology, currentSubTopology, newNode);
    }

    //TODO
    @Override
    public ParallelBranchedKStream<K, V> split() {
        return null;
    }

    @Override
    public void forEach(Consumer<Record<K, V>> forEachConsumer) {
        Node<?, ?, ?, ?> newNode = new ForEachNode<>(forEachConsumer);
        currentSubTopology.addNode(fromNode, newNode);
    }

    @Override
    public void to(String topic) {
        to(List.of(topic));
    }

    //TODO
    @Override
    public void to(Collection<String> topicCollection) {
        //TODO
    }
}

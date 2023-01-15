package io.github.jorgerojasdev.parallelkstream.api;

import io.github.jorgerojasdev.parallelkstream.exception.handler.ParallelKStreamDefaultExceptionHandler;
import io.github.jorgerojasdev.parallelkstream.exception.handler.ParallelKStreamExceptionHandler;
import io.github.jorgerojasdev.parallelkstream.internal.ParallelKStreamImp;
import io.github.jorgerojasdev.parallelkstream.internal.SubTopology;
import io.github.jorgerojasdev.parallelkstream.internal.Topology;
import io.github.jorgerojasdev.parallelkstream.internal.model.node.SourceNode;
import io.github.jorgerojasdev.parallelkstream.internal.model.properties.ParallelKStreamProperties;
import lombok.extern.slf4j.Slf4j;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class ParallelStreamsBuilder {

    private static final String DEFAULT_NAME = "SUBTOPOLOGY";
    private static final AtomicInteger SUBTOPOLOGY_INDEX = new AtomicInteger(0);
    private static final ParallelKStreamExceptionHandler DEFAULT_EXCEPTION_HANDLER = new ParallelKStreamDefaultExceptionHandler();

    private final Topology topology;
    private final ParallelKStreamExceptionHandler globalExceptionHandler;

    public ParallelStreamsBuilder(ParallelKStreamProperties properties) {
        this(properties, DEFAULT_EXCEPTION_HANDLER);
    }

    public ParallelStreamsBuilder(ParallelKStreamProperties properties, ParallelKStreamExceptionHandler globalExceptionHandler) {
        topology = new Topology(properties);
        this.globalExceptionHandler = globalExceptionHandler;
    }

    public <K, V> ParallelKStream<K, V> stream(String topic) {
        return stream(Collections.singleton(topic));
    }

    public <K, V> ParallelKStream<K, V> stream(Collection<String> topics) {
        return stream(topics, createDefaultSubtopologyName());
    }

    public <K, V> ParallelKStream<K, V> stream(String topic, String subTopologyName) {
        return stream(List.of(topic), subTopologyName);
    }

    public <K, V> ParallelKStream<K, V> stream(Collection<String> topics, String subTopologyName) {
        return stream(topics, subTopologyName, this.globalExceptionHandler);
    }

    public <K, V> ParallelKStream<K, V> stream(String topic, ParallelKStreamExceptionHandler exceptionHandler) {
        return stream(List.of(topic), createDefaultSubtopologyName(), exceptionHandler);
    }

    public <K, V> ParallelKStream<K, V> stream(Collection<String> topics, ParallelKStreamExceptionHandler exceptionHandler) {
        return stream(topics, createDefaultSubtopologyName(), exceptionHandler);
    }

    public <K, V> ParallelKStream<K, V> stream(String topic, String subtopologyName, ParallelKStreamExceptionHandler exceptionHandler) {
        return stream(List.of(topic), subtopologyName, exceptionHandler);
    }

    public <K, V> ParallelKStream<K, V> stream(Collection<String> topicCollection, String subtopologyName, ParallelKStreamExceptionHandler exceptionHandler) {
        SUBTOPOLOGY_INDEX.addAndGet(1);
        SourceNode<K, V> sourceNode = new SourceNode<>(topicCollection, topology.getProperties());
        SubTopology<K, V> subTopology = new SubTopology<>(subtopologyName, sourceNode, exceptionHandler, () -> {
            log.info("Pausing all subtopologies...");
            this.topology.pause();
        });
        return new ParallelKStreamImp<>(topology, subTopology, null);
    }

    public Topology build() {
        return topology;
    }

    private String createDefaultSubtopologyName() {
        return String.format("%s-%s", DEFAULT_NAME, SUBTOPOLOGY_INDEX.getAndAdd(0));
    }
}

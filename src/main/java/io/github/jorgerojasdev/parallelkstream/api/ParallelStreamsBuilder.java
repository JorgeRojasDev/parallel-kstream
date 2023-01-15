package io.github.jorgerojasdev.parallelkstream.api;

import io.github.jorgerojasdev.parallelkstream.internal.ParallelKStreamImp;
import io.github.jorgerojasdev.parallelkstream.internal.SubTopology;
import io.github.jorgerojasdev.parallelkstream.internal.Topology;
import io.github.jorgerojasdev.parallelkstream.internal.model.node.SourceNode;
import io.github.jorgerojasdev.parallelkstream.internal.model.properties.ParallelKStreamProperties;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

public class ParallelStreamsBuilder {

    private static final String DEFAULT_NAME = "SUBTOPOLOGY";
    private static final AtomicInteger SUBTOPOLOGY_INDEX = new AtomicInteger(0);

    private final Topology topology;

    public ParallelStreamsBuilder(ParallelKStreamProperties properties) {
        topology = new Topology(properties);
    }

    public <K, V> ParallelKStream<K, V> stream(String topic) {
        return stream(Collections.singleton(topic));
    }

    public <K, V> ParallelKStream<K, V> stream(Collection<String> topicCollection) {
        SUBTOPOLOGY_INDEX.addAndGet(1);
        return new ParallelKStreamImp<>(topology, new SubTopology<>(createDefaultName(), new SourceNode<>(topicCollection, topology.getProperties())), null);
    }

    public Topology build() {
        return topology;
    }

    private String createDefaultName() {
        return String.format("%s-%s", DEFAULT_NAME, SUBTOPOLOGY_INDEX.getAndAdd(0));
    }
}

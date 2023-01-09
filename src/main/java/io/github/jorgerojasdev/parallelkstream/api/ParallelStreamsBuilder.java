package io.github.jorgerojasdev.parallelkstream.api;

import io.github.jorgerojasdev.parallelkstream.internal.ParallelKStreamImp;
import io.github.jorgerojasdev.parallelkstream.internal.SubTopology;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

public class ParallelStreamsBuilder {

    private static final String DEFAULT_NAME = "SUBTOPOLOGY";
    private static final AtomicInteger SUBTOPOLOGY_INDEX = new AtomicInteger(0);

    public <K, V> ParallelKStream<K, V> stream(String topic) {
        return stream(Collections.singleton(topic));
    }

    public <K, V> ParallelKStream<K, V> stream(Collection<String> topicCollection) {
        return new ParallelKStreamImp<>(new SubTopology(createDefaultName()), null);
    }

    private String createDefaultName() {
        return String.format("%s-%s", DEFAULT_NAME, SUBTOPOLOGY_INDEX.getAndAdd(1));
    }
}

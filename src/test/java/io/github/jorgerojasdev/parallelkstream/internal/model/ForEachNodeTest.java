package io.github.jorgerojasdev.parallelkstream.internal.model;

import io.github.jorgerojasdev.parallelkstream.internal.SubTopology;
import io.github.jorgerojasdev.parallelkstream.internal.model.common.Record;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ForEachNodeTest extends AbstractNodeTest {

    @Test
    void whenForEachThenProcessRecord() {
        AtomicInteger counter = new AtomicInteger(0);

        parallelKStream
                .forEach(recordKv -> counter.addAndGet(1));

        SubTopology<?, ?> subTopology = parallelStreamsBuilder.build().subTopologies().get(0);

        IntStream.range(0, 3).forEach(index -> subTopology.
                process(Record.<String, String>builder().key(String.format("key-%s", index)).value(String.format("value-%s", index)).build()));

        assertEquals(3, counter.get());
    }
}

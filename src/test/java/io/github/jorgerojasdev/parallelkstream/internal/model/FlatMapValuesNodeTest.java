package io.github.jorgerojasdev.parallelkstream.internal.model;

import io.github.jorgerojasdev.parallelkstream.api.ParallelKStream;
import io.github.jorgerojasdev.parallelkstream.api.ParallelStreamsBuilder;
import io.github.jorgerojasdev.parallelkstream.internal.model.common.Record;
import io.github.jorgerojasdev.parallelkstream.internal.model.node.KeyValue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class FlatMapValuesNodeTest {

    private ParallelKStream<String, String> parallelKStream;
    private static final Logger LOGGER = LoggerFactory.getLogger(FlatMapValuesNodeTest.class);

    @BeforeEach
    void restoreParallelKStream() {
        parallelKStream = new ParallelStreamsBuilder().stream("test");
    }

    @Test
    void whenFlatMapValuesReturnsThreeValidKeyValuesThenForwardsSameNumberOfMappedValues() {
        List<Record<String, String>> elementsForwarded = new ArrayList<>();

        ParallelKStream<String, String> parallelFilteredKStream = parallelKStream
                .flatMapValues(recordKv -> Stream.of(
                        "value-1",
                        "value-2",
                        "value-3"
                ))
                .map(recordKv -> {
                    elementsForwarded.add(recordKv);
                    return null;
                });

        parallelFilteredKStream.build().
                process(Record.<String, String>builder().key("testkey").value("valueKey").build());

        assertFalse(elementsForwarded.isEmpty());
        assertEquals(3, elementsForwarded.size());
        assertEquals("testkey", elementsForwarded.get(0).getKey());
        assertEquals("value-1", elementsForwarded.get(0).getValue());
        assertEquals("testkey", elementsForwarded.get(1).getKey());
        assertEquals("value-2", elementsForwarded.get(1).getValue());
        assertEquals("testkey", elementsForwarded.get(2).getKey());
        assertEquals("value-3", elementsForwarded.get(2).getValue());
    }

    @Test
    void whenFlatMapReturnsAInvalidKeyValueThenNonForwardsAnything() {
        List<Record<?, ?>> elementsForwarded = new ArrayList<>();

        ParallelKStream<String, String> parallelFilteredKStream = parallelKStream
                .flatMap(recordKv -> Stream.of(
                        KeyValue.pair(null, null),
                        null
                ))
                .map(recordKv -> {
                    elementsForwarded.add(recordKv);
                    return null;
                });

        parallelFilteredKStream.build().
                process(Record.<String, String>builder().key("testkey").value("testValue").build());

        assertTrue(elementsForwarded.isEmpty());
    }
}

package io.github.jorgerojasdev.parallelkstream.internal.model;

import io.github.jorgerojasdev.parallelkstream.api.ParallelKStream;
import io.github.jorgerojasdev.parallelkstream.internal.model.common.KeyValue;
import io.github.jorgerojasdev.parallelkstream.internal.model.common.Record;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class FlatMapNodeTest extends AbstractNodeTest {

    @Test
    void whenFlatMapReturnsThreeValidKeyValuesThenForwardsSameNumberOfMappedValues() {
        List<Record<String, String>> elementsForwarded = new ArrayList<>();

        ParallelKStream<String, String> parallelFilteredKStream = parallelKStream
                .flatMap(recordKv -> Stream.of(
                        KeyValue.pair("key-1", "value-1"),
                        KeyValue.pair("key-2", "value-2"),
                        KeyValue.pair("key-3", "value-3")
                ))
                .map(recordKv -> {
                    elementsForwarded.add(recordKv);
                    return null;
                });

        parallelStreamsBuilder.build().subTopologies().get(0)
                .process(Record.<String, String>builder().key("testkey").value("valueKey").build());

        assertFalse(elementsForwarded.isEmpty());
        assertEquals(3, elementsForwarded.size());
        assertEquals("key-1", elementsForwarded.get(0).getKey());
        assertEquals("value-1", elementsForwarded.get(0).getValue());
        assertEquals("key-2", elementsForwarded.get(1).getKey());
        assertEquals("value-2", elementsForwarded.get(1).getValue());
        assertEquals("key-3", elementsForwarded.get(2).getKey());
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

        parallelStreamsBuilder.build().subTopologies().get(0)
                .process(Record.<String, String>builder().key("testkey").value("testValue").build());

        assertTrue(elementsForwarded.isEmpty());
    }
}

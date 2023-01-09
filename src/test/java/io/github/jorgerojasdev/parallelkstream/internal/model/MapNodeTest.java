package io.github.jorgerojasdev.parallelkstream.internal.model;

import io.github.jorgerojasdev.parallelkstream.api.ParallelKStream;
import io.github.jorgerojasdev.parallelkstream.api.ParallelStreamsBuilder;
import io.github.jorgerojasdev.parallelkstream.internal.model.common.Record;
import io.github.jorgerojasdev.parallelkstream.internal.model.node.KeyValue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class MapNodeTest {

    private ParallelKStream<String, String> parallelKStream;
    private static final Logger LOGGER = LoggerFactory.getLogger(MapNodeTest.class);

    @BeforeEach
    void restoreParallelKStream() {
        parallelKStream = new ParallelStreamsBuilder().stream("test");
    }

    @Test
    void whenMapReturnsValidKeyValueThenForwardsMappedValue() {
        AtomicReference<Record<String, String>> response = new AtomicReference<>();

        ParallelKStream<String, String> parallelFilteredKStream = parallelKStream
                .map(recordKv -> KeyValue.pair("testModified", "valueModified"))
                .map(recordKv -> {
                    response.set(recordKv);
                    return null;
                });

        parallelFilteredKStream.build().
                process(Record.<String, String>builder().key("testkey").value("valueKey").build());

        assertNotNull(response.get());
        assertEquals("testModified", response.get().getKey());
        assertEquals("valueModified", response.get().getValue());
    }

    @Test
    void whenMapReturnsInvalidKeyValueThenNonForwardsAny() {
        AtomicReference<Boolean> forwarded = new AtomicReference<>(false);

        ParallelKStream<String, String> parallelFilteredKStream = parallelKStream
                .map(recordKv -> null)
                .map(recordKv -> {
                    forwarded.set(true);
                    return null;
                });

        parallelFilteredKStream.build().
                process(Record.<String, String>builder().key("testkey").value("valueKey").build());

        assertFalse(forwarded.get());
    }
}

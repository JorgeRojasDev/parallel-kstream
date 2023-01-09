package io.github.jorgerojasdev.parallelkstream.internal.model;

import io.github.jorgerojasdev.parallelkstream.api.ParallelKStream;
import io.github.jorgerojasdev.parallelkstream.api.ParallelStreamsBuilder;
import io.github.jorgerojasdev.parallelkstream.internal.model.common.Record;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class MapValuesNodeTest {

    private ParallelKStream<String, String> parallelKStream;
    private static final Logger LOGGER = LoggerFactory.getLogger(MapValuesNodeTest.class);

    @BeforeEach
    void restoreParallelKStream() {
        parallelKStream = new ParallelStreamsBuilder().stream("test");
    }

    @Test
    void whenMapValuesThenForwardsMappedValue() {
        AtomicReference<Record<String, String>> response = new AtomicReference<>();

        ParallelKStream<String, String> parallelFilteredKStream = parallelKStream
                .mapValues(recordKv -> "valueModified")
                .mapValues(recordKv -> {
                    response.set(recordKv);
                    return null;
                });

        parallelFilteredKStream.build().
                process(Record.<String, String>builder().key("testkey").value("testValue").build());

        assertNotNull(response.get());
        assertEquals("testkey", response.get().getKey());
        assertEquals("valueModified", response.get().getValue());
    }
}

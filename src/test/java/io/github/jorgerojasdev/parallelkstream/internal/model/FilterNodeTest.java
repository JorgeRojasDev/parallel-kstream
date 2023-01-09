package io.github.jorgerojasdev.parallelkstream.internal.model;

import io.github.jorgerojasdev.parallelkstream.api.ParallelKStream;
import io.github.jorgerojasdev.parallelkstream.api.ParallelStreamsBuilder;
import io.github.jorgerojasdev.parallelkstream.internal.model.common.Record;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

class FilterNodeTest {

    private ParallelKStream<String, String> parallelKStream;
    private static final Logger LOGGER = LoggerFactory.getLogger(FilterNodeTest.class);

    @BeforeEach
    void restoreParallelKStream() {
        parallelKStream = new ParallelStreamsBuilder().stream("test");
    }

    @Test
    void whenFilterReturnsTrueThenForwardsValue() {
        AtomicReference<Record<String, String>> response = new AtomicReference<>();

        ParallelKStream<String, String> parallelFilteredKStream = parallelKStream
                .filter(recordKv -> true)
                .filter(recordKv -> {
                    LOGGER.info("Received value: {}", recordKv);
                    response.set(recordKv);
                    return true;
                });

        parallelFilteredKStream.build().
                process(Record.<String, String>builder().key("testkey").value("valueKey").build());

        assertNotNull(response.get());
    }

    @Test
    void whenFilterReturnsFalseThenNotForwardsValue() {
        AtomicReference<Record<String, String>> response = new AtomicReference<>();

        ParallelKStream<String, String> parallelFilteredKStream = parallelKStream
                .filter(recordKv -> false)
                .filter(recordKv -> {
                    LOGGER.info("Received value: {}", recordKv);
                    response.set(recordKv);
                    return false;
                });

        parallelFilteredKStream.build().
                process(Record.<String, String>builder().key("testkey").value("valueKey").build());

        assertNull(response.get());
    }
}

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

class PeekNodeTest {

    private ParallelKStream<String, String> parallelKStream;
    private static final Logger LOGGER = LoggerFactory.getLogger(PeekNodeTest.class);

    @BeforeEach
    void restoreParallelKStream() {
        parallelKStream = new ParallelStreamsBuilder().stream("test");
    }

    @Test
    void whenPeekThenForwardsValue() {
        AtomicReference<Record<String, String>> response = new AtomicReference<>();

        ParallelKStream<String, String> parallelFilteredKStream = parallelKStream
                .peek(recordKv -> LOGGER.info(recordKv.toString()))
                .filter(recordKv -> {
                    response.set(recordKv);
                    return true;
                });

        parallelFilteredKStream.build().
                process(Record.<String, String>builder().key("testkey").value("testValue").build());

        assertNotNull(response.get());
    }
}

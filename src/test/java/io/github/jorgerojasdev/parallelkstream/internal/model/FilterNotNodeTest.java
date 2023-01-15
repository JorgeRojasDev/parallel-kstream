package io.github.jorgerojasdev.parallelkstream.internal.model;

import io.github.jorgerojasdev.parallelkstream.internal.model.common.Record;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

class FilterNotNodeTest extends AbstractNodeTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(FilterNotNodeTest.class);

    @Test
    void whenFilterNotReturnsFalseThenForwardsValue() {
        AtomicReference<Record<String, String>> response = new AtomicReference<>();

        parallelKStream
                .filterNot(recordKv -> false)
                .filterNot(recordKv -> {
                    LOGGER.info("Received value: {}", recordKv);
                    response.set(recordKv);
                    return false;
                });

        parallelStreamsBuilder.build().subTopologies().get(0)
                .process(Record.<String, String>builder().key("testkey").value("valueKey").build());

        assertNotNull(response.get());
    }

    @Test
    void whenFilterNotReturnsTrueThenNotForwardsValue() {
        AtomicReference<Record<String, String>> response = new AtomicReference<>();

        parallelKStream
                .filterNot(recordKv -> true)
                .filterNot(recordKv -> {
                    LOGGER.info("Received value: {}", recordKv);
                    response.set(recordKv);
                    return true;
                });

        parallelStreamsBuilder.build().subTopologies().get(0)
                .process(Record.<String, String>builder().key("testkey").value("valueKey").build());

        assertNull(response.get());
    }
}

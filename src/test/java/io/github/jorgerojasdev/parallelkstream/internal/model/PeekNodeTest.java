package io.github.jorgerojasdev.parallelkstream.internal.model;

import io.github.jorgerojasdev.parallelkstream.internal.model.common.Record;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertNotNull;

class PeekNodeTest extends AbstractNodeTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(PeekNodeTest.class);

    @Test
    void whenPeekThenForwardsValue() {
        AtomicReference<Record<String, String>> response = new AtomicReference<>();

        parallelKStream
                .peek(recordKv -> LOGGER.info(recordKv.toString()))
                .filter(recordKv -> {
                    response.set(recordKv);
                    return true;
                });

        parallelStreamsBuilder.build().subTopologies().get(0)
                .process(Record.<String, String>builder().key("testkey").value("testValue").build());

        assertNotNull(response.get());
    }
}

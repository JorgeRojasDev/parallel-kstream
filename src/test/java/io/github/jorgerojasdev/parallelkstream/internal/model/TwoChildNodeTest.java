package io.github.jorgerojasdev.parallelkstream.internal.model;

import io.github.jorgerojasdev.parallelkstream.internal.model.common.Record;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TwoChildNodeTest extends AbstractNodeTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(TwoChildNodeTest.class);

    @Test
    void whenFilterReturnsTrueThenForwardsValue() {
        AtomicInteger count = new AtomicInteger(0);

        parallelKStream
                .filter(recordKv -> {
                    LOGGER.info("Record primary received");
                    count.addAndGet(1);
                    return true;
                });

        parallelKStream
                .filter(recordKv -> {
                    LOGGER.info("Record secondary received");
                    count.addAndGet(1);
                    return false;
                });

        parallelStreamsBuilder.build().subTopologies().get(0)
                .process(Record.<String, String>builder().key("testkey").value("valueKey").build());

        assertEquals(2, count.get());
    }
}

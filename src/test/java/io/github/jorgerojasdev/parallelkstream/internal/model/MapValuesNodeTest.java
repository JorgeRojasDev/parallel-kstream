package io.github.jorgerojasdev.parallelkstream.internal.model;

import io.github.jorgerojasdev.parallelkstream.internal.model.common.Record;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class MapValuesNodeTest extends AbstractNodeTest {

    @Test
    void whenMapValuesThenForwardsMappedValue() {
        AtomicReference<Record<String, String>> response = new AtomicReference<>();

        parallelKStream
                .mapValues(recordKv -> "valueModified")
                .mapValues(recordKv -> {
                    response.set(recordKv);
                    return null;
                });

        parallelStreamsBuilder.build().subTopologies().get(0)
                .process(Record.<String, String>builder().key("testkey").value("testValue").build());

        assertNotNull(response.get());
        assertEquals("testkey", response.get().getKey());
        assertEquals("valueModified", response.get().getValue());
    }
}

package io.github.jorgerojasdev.parallelkstream.internal.model;

import io.github.jorgerojasdev.parallelkstream.internal.model.common.KeyValue;
import io.github.jorgerojasdev.parallelkstream.internal.model.common.Record;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class MapNodeTest extends AbstractNodeTest {

    @Test
    void whenMapReturnsValidKeyValueThenForwardsMappedValue() {
        AtomicReference<Record<String, String>> response = new AtomicReference<>();

        parallelKStream
                .map(recordKv -> KeyValue.pair("testModified", "valueModified"))
                .map(recordKv -> {
                    response.set(recordKv);
                    return null;
                });

        parallelStreamsBuilder.build().subTopologies().get(0)
                .process(Record.<String, String>builder().key("testkey").value("valueKey").build());

        assertNotNull(response.get());
        assertEquals("testModified", response.get().getKey());
        assertEquals("valueModified", response.get().getValue());
    }

    @Test
    void whenMapReturnsInvalidKeyValueThenNonForwardsAny() {
        AtomicReference<Boolean> forwarded = new AtomicReference<>(false);

        parallelKStream
                .map(recordKv -> null)
                .map(recordKv -> {
                    forwarded.set(true);
                    return null;
                });

        parallelStreamsBuilder.build().subTopologies().get(0)
                .process(Record.<String, String>builder().key("testkey").value("valueKey").build());

        assertFalse(forwarded.get());
    }
}

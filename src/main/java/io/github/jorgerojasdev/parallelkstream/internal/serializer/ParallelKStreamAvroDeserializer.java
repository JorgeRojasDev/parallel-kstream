package io.github.jorgerojasdev.parallelkstream.internal.serializer;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.github.jorgerojasdev.parallelkstream.exception.handler.ParallelKStreamExceptionHandler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.kafka.common.KafkaException;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@RequiredArgsConstructor
public class ParallelKStreamAvroDeserializer extends KafkaAvroDeserializer {

    private final ParallelKStreamExceptionHandler exceptionHandler;
    private boolean isKey;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        this.isKey = isKey;

        Map<String, Object> modifiedConfigs = new HashMap<>(configs);
        modifiedConfigs.put("specific.avro.reader", true);

        this.configure(new KafkaAvroDeserializerConfig(modifiedConfigs));
    }

    @Override
    public Object deserialize(String s, byte[] bytes, Schema readerSchema) {
        try {
            return this.deserialize(bytes, readerSchema);
        } catch (KafkaException e) {
            log.debug("Error deserializing object", e);
            return DeserializationError.builder()
                    .kafkaException(e)
                    .build();
        }
    }
}

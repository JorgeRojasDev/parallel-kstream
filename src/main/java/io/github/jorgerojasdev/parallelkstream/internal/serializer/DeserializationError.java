package io.github.jorgerojasdev.parallelkstream.internal.serializer;

import lombok.Builder;
import lombok.Getter;
import org.apache.kafka.common.KafkaException;

@Builder
@Getter
public class DeserializationError {

    private final KafkaException kafkaException;
}

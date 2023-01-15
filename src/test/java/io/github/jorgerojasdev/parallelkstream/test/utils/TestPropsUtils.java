package io.github.jorgerojasdev.parallelkstream.test.utils;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Map;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class TestPropsUtils {

    public static Map<String, Object> defaultProps() {
        return Map.ofEntries(
                Map.entry(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, true),
                Map.entry(ConsumerConfig.GROUP_ID_CONFIG, "test"),
                Map.entry(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"),
                Map.entry(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest"),
                Map.entry(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer"),
                Map.entry(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer"),
                Map.entry(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer"),
                Map.entry(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
        );
    }
}

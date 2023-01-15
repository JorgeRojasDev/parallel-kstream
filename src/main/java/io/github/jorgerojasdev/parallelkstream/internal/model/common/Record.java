package io.github.jorgerojasdev.parallelkstream.internal.model.common;

import io.github.jorgerojasdev.parallelkstream.exception.DeserializationException;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;

@Builder
@Getter
@EqualsAndHashCode
@ToString
public class Record<K, V> {

    private final String topic;
    private final int partition;
    private final long offset;
    private final long timestamp;
    private final Headers headers;
    private final K key;
    private final V value;

    public static <K, V, NV> Record<K, NV> fromAnotherRecord(Record<K, V> kvRecord, NV value) {
        return fromAnotherRecord(kvRecord, KeyValue.pair(kvRecord.getKey(), value));
    }

    public static <K, V, NK, NV> Record<NK, NV> fromAnotherRecord(Record<K, V> kvRecord, KeyValue<NK, NV> keyValue) {
        return Record.<NK, NV>builder()
                .topic(kvRecord.getTopic())
                .partition(kvRecord.getPartition())
                .offset(kvRecord.getOffset())
                .timestamp(kvRecord.getTimestamp())
                .headers(kvRecord.getHeaders())
                .key(keyValue.getKey())
                .value(keyValue.getValue())
                .build();
    }

    public static <K, V> Record<K, V> fromConsumerRecord(ConsumerRecord<K, V> consumerRecord) {
        try {
            K key = consumerRecord.key();
            V value = consumerRecord.value();
            return Record.<K, V>builder()
                    .key(key)
                    .value(value)
                    .headers(consumerRecord.headers())
                    .timestamp(consumerRecord.timestamp())
                    .partition(consumerRecord.partition())
                    .offset(consumerRecord.offset())
                    .topic(consumerRecord.topic())
                    .build();
        } catch (Throwable e) {
            throw new DeserializationException(e.getMessage(), e);
        }
    }
}

package io.github.jorgerojasdev.parallelkstream.internal.model.node;

import io.github.jorgerojasdev.parallelkstream.exception.ProducerException;
import io.github.jorgerojasdev.parallelkstream.internal.model.common.KeyValue;
import io.github.jorgerojasdev.parallelkstream.internal.model.common.Record;
import io.github.jorgerojasdev.parallelkstream.utils.NodeUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class SinkNode<K, V> extends Node<K, V, K, V> {

    public static final String DEFAULT_NAME = "PARALLEL-KSTREAM-SINK";

    public SinkNode(String topic, Map<String, Object> producerProperties) {
        this(List.of(topic), producerProperties);
    }

    public SinkNode(Collection<String> topics, Map<String, Object> producerProperties) {
        this(NodeUtils.defaultNodeName(DEFAULT_NAME), topics, producerProperties);
    }

    public SinkNode(String nodeName, Collection<String> topics, Map<String, Object> producerProperties) {
        super(nodeName, sinkFunction(new KafkaProducer<>(producerProperties), topics));
    }

    private static <K, V> Function<Record<K, V>, List<KeyValue<K, V>>> sinkFunction(Producer<K, V> producer, Collection<String> topics) {
        return recordKv -> {
            try {
                topics.forEach(topic -> producer.send(new ProducerRecord<>(topic, recordKv.getKey(), recordKv.getValue())));
            } catch (KafkaException e) {
                throw new ProducerException(e.getMessage(), e);
            }
            return Collections.emptyList();
        };
    }

}

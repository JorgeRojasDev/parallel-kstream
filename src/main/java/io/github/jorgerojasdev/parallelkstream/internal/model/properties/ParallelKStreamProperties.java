package io.github.jorgerojasdev.parallelkstream.internal.model.properties;

import io.github.jorgerojasdev.parallelkstream.internal.model.config.ParallelConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class ParallelKStreamProperties {

    private final Map<String, Object> properties = new HashMap<>();

    public ParallelKStreamProperties() {
        //Default constructor
    }

    public ParallelKStreamProperties(Map<String, Object> properties) {
        putAll(properties);
    }

    public static ParallelKStreamProperties create() {
        return new ParallelKStreamProperties();
    }

    public static ParallelKStreamProperties create(Map<String, Object> properties) {
        return new ParallelKStreamProperties(properties);
    }

    public void put(String key, Object value) {
        properties.put(key, value);
    }

    public void putAll(Map<String, Object> propertiesToAdd) {
        properties.putAll(propertiesToAdd);
    }

    public Map<String, Object> consumerProps() {
        Map<String, Object> consumerProps = properties.entrySet().stream()
                .filter(entry -> ConsumerConfig.configNames().contains(entry.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        noAssignedProps().forEach(consumerProps::put);

        return consumerProps;
    }

    public Map<String, Object> producerProps() {
        Map<String, Object> producerProps = properties.entrySet().stream()
                .filter(entry -> ProducerConfig.configNames().contains(entry.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        noAssignedProps().forEach(producerProps::put);

        return producerProps;
    }

    public Map<String, Object> parallelProps() {
        Map<String, Object> parallelProps = properties.entrySet().stream()
                .filter(entry -> ParallelConsumerConfig.configNames().contains(entry.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        noAssignedProps().forEach(parallelProps::put);

        return parallelProps;
    }

    public Map<String, Object> noAssignedProps() {
        return properties.entrySet().stream()
                .filter(entry -> !ConsumerConfig.configNames().contains(entry.getKey()) &&
                        !ProducerConfig.configNames().contains(entry.getKey()) &&
                        !ParallelConsumerConfig.configNames().contains(entry.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }
}

package io.github.jorgerojasdev.parallelkstream.utils;

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.github.jorgerojasdev.parallelkstream.internal.model.config.ParallelConsumerConfig;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ParallelConsumerPropsUtilsTest {

    @Test
    void commitModeDefault() {
        Map<String, Object> properties = new HashMap<>();
        assertEquals(ParallelConsumerOptions.CommitMode.PERIODIC_CONSUMER_SYNC, ParallelConsumerPropsUtils.getCommitMode(properties));
    }

    @Test
    void commitModePeriodicConsumerAsynchronous() {
        Map<String, Object> properties = new HashMap<>();
        properties.put(ParallelConsumerConfig.COMMIT_MODE_CONFIG, "periodic_consumer_asynchronous");
        assertEquals(ParallelConsumerOptions.CommitMode.PERIODIC_CONSUMER_ASYNCHRONOUS, ParallelConsumerPropsUtils.getCommitMode(properties));
    }

    @Test
    void commitModePeriodicTransactionalProducer() {
        Map<String, Object> properties = new HashMap<>();
        properties.put(ParallelConsumerConfig.COMMIT_MODE_CONFIG, "periodic_transactional_producer");
        assertEquals(ParallelConsumerOptions.CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER, ParallelConsumerPropsUtils.getCommitMode(properties));
    }

    @Test
    void maxConcurrencyDefault() {
        Map<String, Object> properties = new HashMap<>();
        assertEquals(2, ParallelConsumerPropsUtils.getMaxConcurrency(properties));
    }

    @Test
    void maxConcurrencyLessThanOne() {
        Map<String, Object> properties = new HashMap<>();
        properties.put(ParallelConsumerConfig.CONCURRENCY_CONFIG, 0);
        assertEquals(2, ParallelConsumerPropsUtils.getMaxConcurrency(properties));
    }

    @Test
    void maxConcurrencyGreaterEqualsThanOne() {
        Map<String, Object> properties = new HashMap<>();
        properties.put(ParallelConsumerConfig.CONCURRENCY_CONFIG, 6);
        assertEquals(6, ParallelConsumerPropsUtils.getMaxConcurrency(properties));
    }

    @Test
    void maxProcessingOrderDefault() {
        Map<String, Object> properties = new HashMap<>();
        assertEquals(ParallelConsumerOptions.ProcessingOrder.KEY, ParallelConsumerPropsUtils.getProcessingOrder(properties));
    }

    @Test
    void maxProcessingOrderPartition() {
        Map<String, Object> properties = new HashMap<>();
        properties.put(ParallelConsumerConfig.PROCESSING_ORDER_CONFIG, "partition");
        assertEquals(ParallelConsumerOptions.ProcessingOrder.PARTITION, ParallelConsumerPropsUtils.getProcessingOrder(properties));
    }

    @Test
    void maxProcessingOrderUnordered() {
        Map<String, Object> properties = new HashMap<>();
        properties.put(ParallelConsumerConfig.PROCESSING_ORDER_CONFIG, "unordered");
        assertEquals(ParallelConsumerOptions.ProcessingOrder.UNORDERED, ParallelConsumerPropsUtils.getProcessingOrder(properties));
    }
}
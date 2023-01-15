package io.github.jorgerojasdev.parallelkstream.utils;

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.github.jorgerojasdev.parallelkstream.internal.model.config.ParallelConsumerConfig;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ParallelConsumerPropsUtils {

    private static final ParallelConsumerOptions.CommitMode DEFAULT_COMMIT_MODE = ParallelConsumerOptions.CommitMode.PERIODIC_CONSUMER_SYNC;
    private static final Integer DEFAULT_MAX_CONCURRENCY = 2;
    private static final ParallelConsumerOptions.ProcessingOrder DEFAULT_PROCESSING_ORDER = ParallelConsumerOptions.ProcessingOrder.KEY;

    public static ParallelConsumerOptions.CommitMode getCommitMode(Map<String, Object> parallelConsumerProps) {
        Object commitModeObject = parallelConsumerProps.get(ParallelConsumerConfig.COMMIT_MODE_CONFIG);

        if (commitModeObject == null) {
            return DEFAULT_COMMIT_MODE;
        }

        try {
            return ParallelConsumerOptions.CommitMode.valueOf(commitModeObject.toString().toUpperCase());
        } catch (IllegalArgumentException e) {
            String logOutput = String.format("Commit mode %s not found, assigning default value: %s", commitModeObject, DEFAULT_COMMIT_MODE.name().toLowerCase());
            log.debug(logOutput, e);
            return DEFAULT_COMMIT_MODE;
        }
    }

    public static Integer getMaxConcurrency(Map<String, Object> parallelConsumerProps) {
        Object concurrencyObject = parallelConsumerProps.get(ParallelConsumerConfig.CONCURRENCY_CONFIG);

        if (concurrencyObject == null) {
            return DEFAULT_MAX_CONCURRENCY;
        }


        try {
            int maxConcurrency = Integer.parseInt(concurrencyObject.toString());
            if (maxConcurrency < 1) {
                throw new NumberFormatException("Concurrency must be positive");
            }
            return maxConcurrency;
        } catch (NumberFormatException e) {
            String logOutput = String.format("Max concurrency invalid: %s, assigning default value: %s", concurrencyObject, DEFAULT_MAX_CONCURRENCY);
            log.debug(logOutput, e);
            return DEFAULT_MAX_CONCURRENCY;
        }
    }

    public static ParallelConsumerOptions.ProcessingOrder getProcessingOrder(Map<String, Object> parallelConsumerProps) {
        Object processingOrderObject = parallelConsumerProps.get(ParallelConsumerConfig.PROCESSING_ORDER_CONFIG);

        if (processingOrderObject == null) {
            return DEFAULT_PROCESSING_ORDER;
        }

        try {
            return ParallelConsumerOptions.ProcessingOrder.valueOf(processingOrderObject.toString().toUpperCase());
        } catch (IllegalArgumentException e) {
            String logOutput = String.format("Processing order %s not found, assigning default value: %s", processingOrderObject, DEFAULT_PROCESSING_ORDER.name().toLowerCase());
            log.debug(logOutput, e);
            return DEFAULT_PROCESSING_ORDER;
        }
    }
}

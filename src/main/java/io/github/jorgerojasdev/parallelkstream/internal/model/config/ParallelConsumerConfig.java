package io.github.jorgerojasdev.parallelkstream.internal.model.config;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.Set;

@Getter
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ParallelConsumerConfig {
    public static final String COMMIT_MODE_CONFIG = "parallel.commit.mode";
    public static final String CONCURRENCY_CONFIG = "parallel.concurrency";
    public static final String PROCESSING_ORDER_CONFIG = "parallel.processing.order";

    public static Set<String> configNames() {
        return Set.of(COMMIT_MODE_CONFIG, CONCURRENCY_CONFIG, PROCESSING_ORDER_CONFIG);
    }
}

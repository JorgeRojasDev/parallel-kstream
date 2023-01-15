package io.github.jorgerojasdev.parallelkstream.internal.model;

import io.github.jorgerojasdev.parallelkstream.api.ParallelKStream;
import io.github.jorgerojasdev.parallelkstream.api.ParallelStreamsBuilder;
import io.github.jorgerojasdev.parallelkstream.internal.model.properties.ParallelKStreamProperties;
import io.github.jorgerojasdev.parallelkstream.test.utils.TestPropsUtils;
import org.junit.jupiter.api.BeforeEach;

public abstract class AbstractNodeTest {
    protected ParallelStreamsBuilder parallelStreamsBuilder;
    protected ParallelKStream<String, String> parallelKStream;

    @BeforeEach
    final void restoreParallelKStream() {
        parallelStreamsBuilder = new ParallelStreamsBuilder(ParallelKStreamProperties.create(TestPropsUtils.defaultProps()));
        parallelKStream = parallelStreamsBuilder.stream("test");
    }
}

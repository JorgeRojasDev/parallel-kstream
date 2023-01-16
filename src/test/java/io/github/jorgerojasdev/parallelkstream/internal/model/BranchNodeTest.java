package io.github.jorgerojasdev.parallelkstream.internal.model;

import io.github.jorgerojasdev.parallelkstream.api.ParallelBranchedDefaultKStream;
import io.github.jorgerojasdev.parallelkstream.internal.model.common.Branch;
import io.github.jorgerojasdev.parallelkstream.internal.model.common.Record;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

class BranchNodeTest extends AbstractNodeTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(BranchNodeTest.class);
    private static final String LEFT_VALUE = "left";
    private static final String RIGHT_VALUE = "right";

    @Test
    void whenBranchLeftIsTrueThenForwardsLeftValue() {
        AtomicReference<Record<String, String>> leftValue = new AtomicReference<>();
        AtomicReference<Record<String, String>> rightValue = new AtomicReference<>();
        AtomicReference<Record<String, String>> defaultValue = new AtomicReference<>();

        createBranchWithDefaultValueTopology(leftValue, rightValue, defaultValue);

        parallelStreamsBuilder.build().subTopologies().get(0)
                .process(Record.<String, String>builder().key("testkey").value(LEFT_VALUE).build());

        assertNotNull(leftValue.get());
        assertNull(rightValue.get());
        assertNull(defaultValue.get());
    }

    @Test
    void whenBranchRightIsTrueThenForwardsRightValue() {
        AtomicReference<Record<String, String>> leftValue = new AtomicReference<>();
        AtomicReference<Record<String, String>> rightValue = new AtomicReference<>();
        AtomicReference<Record<String, String>> defaultValue = new AtomicReference<>();

        createBranchWithDefaultValueTopology(leftValue, rightValue, defaultValue);

        parallelStreamsBuilder.build().subTopologies().get(0)
                .process(Record.<String, String>builder().key("testkey").value(RIGHT_VALUE).build());

        assertNull(leftValue.get());
        assertNotNull(rightValue.get());
        assertNull(defaultValue.get());
    }

    @Test
    void whenAllBranchAreFalseThenForwardsDefaultValue() {
        AtomicReference<Record<String, String>> leftValue = new AtomicReference<>();
        AtomicReference<Record<String, String>> rightValue = new AtomicReference<>();
        AtomicReference<Record<String, String>> defaultValue = new AtomicReference<>();

        createBranchWithDefaultValueTopology(leftValue, rightValue, defaultValue);

        parallelStreamsBuilder.build().subTopologies().get(0)
                .process(Record.<String, String>builder().key("testkey").value("random_value").build());

        assertNull(leftValue.get());
        assertNull(rightValue.get());
        assertNotNull(defaultValue.get());
    }

    private void createBranchWithDefaultValueTopology(AtomicReference<Record<String, String>> leftValue,
                                                      AtomicReference<Record<String, String>> rightValue,
                                                      AtomicReference<Record<String, String>> defaultValue) {

        ParallelBranchedDefaultKStream<String, String> parallelKStreamMapWithDefaultBranch = parallelKStream
                .split()
                .branch(Branch.create(recordKv -> LEFT_VALUE.equals(recordKv.getValue()), LEFT_VALUE))
                .branch(Branch.create(recordKv -> RIGHT_VALUE.equals(recordKv.getValue()), RIGHT_VALUE))
                .defaultBranch();

        parallelKStreamMapWithDefaultBranch
                .get(LEFT_VALUE)
                .filter(recordKv -> {
                    leftValue.set(recordKv);
                    return true;
                });

        parallelKStreamMapWithDefaultBranch
                .get(RIGHT_VALUE)
                .filter(recordKv -> {
                    rightValue.set(recordKv);
                    return true;
                });

        parallelKStreamMapWithDefaultBranch
                .defaultBranch()
                .filter(recordKv -> {
                    defaultValue.set(recordKv);
                    return true;
                });
    }
}

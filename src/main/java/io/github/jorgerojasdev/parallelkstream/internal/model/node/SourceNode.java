package io.github.jorgerojasdev.parallelkstream.internal.model.node;

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelStreamProcessor;
import io.confluent.parallelconsumer.PollContext;
import io.github.jorgerojasdev.parallelkstream.exception.TopologyException;
import io.github.jorgerojasdev.parallelkstream.internal.model.properties.ParallelKStreamProperties;
import io.github.jorgerojasdev.parallelkstream.utils.ParallelConsumerPropsUtils;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collection;
import java.util.Map;

@Getter
@Slf4j
public class SourceNode<K, V> {
    private final Collection<String> topicCollection;
    private final ParallelKStreamProperties properties;
    private ParallelStreamProcessor<K, V> processor;

    public SourceNode(Collection<String> topicCollection, ParallelKStreamProperties properties) {
        if (topicCollection == null || topicCollection.isEmpty()) {
            throw new TopologyException("Source Node must to have any topic");
        }
        this.topicCollection = topicCollection;
        this.properties = properties;
    }

    public void init() {
        this.processor = createParallelStreamProcessor(topicCollection, properties);
    }

    public void start(java.util.function.Consumer<PollContext<K, V>> pollContextConsumer) {
        initIfUninitialized();
        processor.poll(pollContextConsumer);
    }

    public void pause() {
        initIfUninitialized();
        processor.pauseIfRunning();
    }

    public void resume() {
        initIfUninitialized();
        processor.resumeIfPaused();
    }

    private void initIfUninitialized() {
        if (processor == null) {
            init();
        }
    }

    private ParallelStreamProcessor<K, V> createParallelStreamProcessor(Collection<String> topicCollection,
                                                                        ParallelKStreamProperties properties) {
        Consumer<K, V> consumer = new KafkaConsumer<>(consumerProperties(properties));

        Map<String, Object> parallelProps = properties.parallelProps();

        ParallelConsumerOptions<K, V> parallelConsumerOptions = ParallelConsumerOptions.<K, V>builder()
                .consumer(consumer)
                .commitMode(ParallelConsumerPropsUtils.getCommitMode(parallelProps))
                .maxConcurrency(ParallelConsumerPropsUtils.getMaxConcurrency(parallelProps))
                .ordering(ParallelConsumerPropsUtils.getProcessingOrder(parallelProps))
                .build();

        ParallelStreamProcessor<K, V> parallelStreamProcessor = ParallelStreamProcessor.createEosStreamProcessor(parallelConsumerOptions);
        parallelStreamProcessor.subscribe(topicCollection);

        return parallelStreamProcessor;
    }

    private Map<String, Object> consumerProperties(ParallelKStreamProperties properties) {
        Map<String, Object> consumerProps = properties.consumerProps();

        Object enableAutoCommit = consumerProps.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG);
        if (enableAutoCommit == null || Boolean.parseBoolean(enableAutoCommit.toString())) {
            log.warn("{} property must to be false, changing", ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG);
            consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        }

        return consumerProps;
    }
}

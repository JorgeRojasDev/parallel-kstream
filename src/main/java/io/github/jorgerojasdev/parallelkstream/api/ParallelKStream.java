package io.github.jorgerojasdev.parallelkstream.api;

import io.github.jorgerojasdev.parallelkstream.internal.SubTopology;
import io.github.jorgerojasdev.parallelkstream.internal.model.common.Record;
import io.github.jorgerojasdev.parallelkstream.internal.model.node.KeyValue;
import io.github.jorgerojasdev.parallelkstream.internal.model.state.ProcessorContext;

import java.util.Collection;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

public interface ParallelKStream<K, V> {

    ParallelKStream<K, V> filter(Predicate<Record<K, V>> predicate);

    ParallelKStream<K, V> filterNot(Predicate<Record<K, V>> predicate);

    <NK, NV> ParallelKStream<NK, NV> map(Function<Record<K, V>, KeyValue<NK, NV>> mapFunction);

    <NV> ParallelKStream<K, NV> mapValues(Function<Record<K, V>, NV> mapValueFunction);

    <NK, NV> ParallelKStream<NK, NV> flatMap(Function<Record<K, V>, Stream<KeyValue<NK, NV>>> flatMapFunction);

    <NV> ParallelKStream<K, NV> flatMapValues(Function<Record<K, V>, Stream<NV>> flatMapFunction);

    <NK, NV> ParallelKStream<NK, NV> transform(BiFunction<ProcessorContext, Record<K, V>, KeyValue<NK, NV>> transformFunction);

    <NV> ParallelKStream<K, NV> transformValues(BiFunction<ProcessorContext, Record<K, V>, NV> transformValuesFunction);

    ParallelKStream<K, V> peek(Consumer<Record<K, V>> peekConsumer);

    ParallelBranchedKStream<K, V> split();

    void forEach(Consumer<Record<K, V>> peekConsumer);

    void to(String topic);

    void to(Collection<String> topicCollection);

    SubTopology build();
}

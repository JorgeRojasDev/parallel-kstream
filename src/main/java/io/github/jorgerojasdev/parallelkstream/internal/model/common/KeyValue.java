package io.github.jorgerojasdev.parallelkstream.internal.model.common;

import lombok.Getter;

@Getter
public class KeyValue<K, V> {

    private final K key;
    private final V value;

    private KeyValue(K key, V value) {
        this.key = key;
        this.value = value;
    }

    public static <K, V> KeyValue<K, V> pair(K key, V value) {
        return new KeyValue<>(key, value);
    }
}

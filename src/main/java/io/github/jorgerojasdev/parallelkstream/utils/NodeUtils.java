package io.github.jorgerojasdev.parallelkstream.utils;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;

import java.util.concurrent.atomic.AtomicInteger;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class NodeUtils {

    private static final AtomicInteger COUNT = new AtomicInteger(0);

    public static String defaultNodeName(String defaultName) {
        return String.format("%s-%s", defaultName, COUNT.addAndGet(1));
    }
}

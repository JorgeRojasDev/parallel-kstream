package io.github.jorgerojasdev.parallelkstream.exception;

public class DeserializationException extends RuntimeException {

    public DeserializationException(String message) {
        super(message);
    }

    public DeserializationException(String message, Throwable e) {
        super(message, e);
    }
}

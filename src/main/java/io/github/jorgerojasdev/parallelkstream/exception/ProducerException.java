package io.github.jorgerojasdev.parallelkstream.exception;

public class ProducerException extends RuntimeException {
    public ProducerException(String message) {
        super(message);
    }

    public ProducerException(String message, Throwable e) {
        super(message, e);
    }
}

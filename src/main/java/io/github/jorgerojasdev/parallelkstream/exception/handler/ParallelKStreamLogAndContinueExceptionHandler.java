package io.github.jorgerojasdev.parallelkstream.exception.handler;

import io.confluent.parallelconsumer.PollContext;
import io.github.jorgerojasdev.parallelkstream.exception.handler.action.ExceptionHandlerAction;
import io.github.jorgerojasdev.parallelkstream.internal.model.common.Record;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ParallelKStreamLogAndContinueExceptionHandler implements ParallelKStreamExceptionHandler {

    @Override
    public ExceptionHandlerAction deserializeExceptionHandle(Throwable e, PollContext<?, ?> pollContext) {
        String outputLog = String.format("DeserializationException controlled by ParallelKStreamLogAndContinueExceptionHandler, continuing record: %s", pollContext.getSingleConsumerRecord());
        log.warn(outputLog);
        return ExceptionHandlerAction.CONTINUE;
    }

    @Override
    public ExceptionHandlerAction uncaughtExceptionHandle(Throwable e, Record<?, ?> recordKv) {
        String outputLog = String.format("UncaughtException controlled by ParallelKStreamLogAndContinueExceptionHandler, continuing record: %s", recordKv);
        log.warn(outputLog);
        return ExceptionHandlerAction.CONTINUE;
    }

    @Override
    public ExceptionHandlerAction produceExceptionHandle(Throwable e, Record<?, ?> recordKv) {
        String outputLog = String.format("ProducerException controlled by ParallelKStreamLogAndContinueExceptionHandler, continuing record: %s", recordKv);
        log.warn(outputLog);
        return ExceptionHandlerAction.CONTINUE;
    }
}

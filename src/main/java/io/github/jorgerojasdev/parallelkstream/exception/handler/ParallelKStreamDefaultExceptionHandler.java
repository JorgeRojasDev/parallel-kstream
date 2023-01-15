package io.github.jorgerojasdev.parallelkstream.exception.handler;

import io.confluent.parallelconsumer.PollContext;
import io.github.jorgerojasdev.parallelkstream.exception.handler.action.ExceptionHandlerAction;
import io.github.jorgerojasdev.parallelkstream.internal.model.common.Record;

public class ParallelKStreamDefaultExceptionHandler implements ParallelKStreamExceptionHandler {
    @Override
    public ExceptionHandlerAction deserializeExceptionHandle(Throwable e, PollContext<?, ?> pollContext) {
        return ExceptionHandlerAction.STOP_APPLICATION;
    }

    @Override
    public ExceptionHandlerAction uncaughtExceptionHandle(Throwable e, Record<?, ?> recordKv) {
        return ExceptionHandlerAction.STOP_APPLICATION;
    }

    @Override
    public ExceptionHandlerAction produceExceptionHandle(Throwable e, Record<?, ?> recordKv) {
        return ExceptionHandlerAction.STOP_APPLICATION;
    }
}

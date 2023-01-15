package io.github.jorgerojasdev.parallelkstream.exception.handler;

import io.confluent.parallelconsumer.PollContext;
import io.github.jorgerojasdev.parallelkstream.exception.handler.action.ExceptionHandlerAction;
import io.github.jorgerojasdev.parallelkstream.internal.model.common.Record;

public interface ParallelKStreamExceptionHandler {

    ExceptionHandlerAction deserializeExceptionHandle(Throwable e, PollContext<?, ?> pollContext);

    ExceptionHandlerAction uncaughtExceptionHandle(Throwable e, Record<?, ?> recordKv);

    ExceptionHandlerAction produceExceptionHandle(Throwable e, Record<?, ?> recordKv);
}

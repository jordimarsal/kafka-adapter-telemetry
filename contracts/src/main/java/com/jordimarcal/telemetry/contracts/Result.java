package com.jordimarcal.telemetry.contracts;

import java.util.function.Function;

/**
 * Result pattern for expected outcomes: validation, duplicates, unknown profiles.
 * Exceptions remain reserved for the exceptional (broker down, DB unreachable).
 *
 * @param <T> success value type
 * @param <E> error value type
 */
public sealed interface Result<T, E> permits Result.Ok, Result.Err {

    static <T, E> Result<T, E> ok(T value) {
        return new Ok<>(value);
    }

    static <T, E> Result<T, E> err(E error) {
        return new Err<>(error);
    }

    default boolean isOk() {
        return this instanceof Ok<T, E>;
    }

    default boolean isErr() {
        return !isOk();
    }

    T orElseThrow();

    E error();

    <U> U fold(Function<? super T, ? extends U> onOk, Function<? super E, ? extends U> onErr);

    record Ok<T, E>(T value) implements Result<T, E> {

        @Override
        public T orElseThrow() {
            return value;
        }

        @Override
        public E error() {
            throw new IllegalStateException("no error in Ok");
        }

        @Override
        public <U> U fold(Function<? super T, ? extends U> onOk, Function<? super E, ? extends U> onErr) {
            return onOk.apply(value);
        }
    }

    record Err<T, E>(E error) implements Result<T, E> {

        @Override
        public T orElseThrow() {
            throw new IllegalStateException("no value in Err: " + error);
        }

        @Override
        public E error() {
            return error;
        }

        @Override
        public <U> U fold(Function<? super T, ? extends U> onOk, Function<? super E, ? extends U> onErr) {
            return onErr.apply(error);
        }
    }
}

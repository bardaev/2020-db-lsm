package ru.mail.polis.hw2;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.ByteBuffer;
import java.util.Optional;

public class Value implements Comparable<Value> {

    private final long timestamp;

    @Nullable
    private final Optional<ByteBuffer> value;

    Value(final long timestamp, @Nullable final ByteBuffer value) {
        assert timestamp > 0L;
        this.timestamp = timestamp;
        this.value = Optional.of(value);
    }

    Value(final long timestamp) {
        assert timestamp > 0L;
        this.timestamp = timestamp;
        this.value = Optional.empty();
    }

    boolean isTombstone() {
        return value.isEmpty();
    }

    @NotNull
    ByteBuffer getData() {
        assert !isTombstone();
        return value.orElseThrow().asReadOnlyBuffer();
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public int compareTo(@NotNull Value o) {
        return -Long.compare(timestamp, o.timestamp);
    }
}
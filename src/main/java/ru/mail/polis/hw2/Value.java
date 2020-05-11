package ru.mail.polis.hw2;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.ByteBuffer;
import java.util.Optional;

public class Value implements Comparable<Value> {

    private final long timestamp;

    @Nullable
    private final Optional<ByteBuffer> val;

    Value(final long timestamp, @Nullable final ByteBuffer value) {
        assert timestamp > 0L;
        this.timestamp = timestamp;
        this.val = Optional.of(value);
    }

    Value(final long timestamp) {
        assert timestamp > 0L;
        this.timestamp = timestamp;
        this.val = Optional.empty();
    }

    boolean isTombstone() {
        return val.isEmpty();
    }

    @NotNull
    ByteBuffer getData() {
        assert !isTombstone();
        return val.orElseThrow().asReadOnlyBuffer();
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public int compareTo(@NotNull final Value o) {
        return -Long.compare(timestamp, o.timestamp);
    }
}

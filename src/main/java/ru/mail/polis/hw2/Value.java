package ru.mail.polis.hw2;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

public class Value implements Comparable<Value> {

    private final ByteBuffer value;
    private final long version;
    private final boolean rip;

    Value(ByteBuffer value, boolean rip) {
        this.value = value;
        this.rip = rip;
        this.version = System.currentTimeMillis();
    }

    public ByteBuffer getValue() {
        return value;
    }

    public long getVersion() {
        return version;
    }

    public boolean isRip() {
        return rip;
    }

    @Override
    public int compareTo(@NotNull Value o) {
        return Long.compare(o.getVersion(), this.getVersion());
    }
}

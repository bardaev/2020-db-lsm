package ru.mail.polis.hw2;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

public class Cell implements Comparable<Cell> {

    private final ByteBuffer key;
    private final Value value;

    Cell(@NotNull final ByteBuffer key, @NotNull final Value value) {
        this.key = key;
        this.value = value;
    }

    public static Cell of(@NotNull final ByteBuffer key, @NotNull final Value value) {
        return new Cell(key, value);
    }

    public ByteBuffer getKey() {
        return key.asReadOnlyBuffer();
    }

    public Value getValue() {
        return value;
    }

    @Override
    public int compareTo(@NotNull Cell o) {
        return 0;
    }
}

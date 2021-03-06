package ru.mail.polis.hw2;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.Iterator;

public interface Table {

    void upsert(@NotNull ByteBuffer key, @NotNull ByteBuffer value);

    void remove(@NotNull ByteBuffer key);

    @NotNull
    Iterator<Cell> iterator(@NotNull ByteBuffer from);
}

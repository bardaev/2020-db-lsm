package ru.mail.polis.hw2;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.Iterator;

public interface Table {

    void upsert(@NotNull ByteBuffer k, @NotNull ByteBuffer v);

    void remove(@NotNull ByteBuffer k);

    @NotNull
    Iterator iterator(@NotNull ByteBuffer from);
}

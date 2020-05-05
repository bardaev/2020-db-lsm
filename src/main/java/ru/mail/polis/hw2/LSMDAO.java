package ru.mail.polis.hw2;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.mail.polis.DAO;
import ru.mail.polis.Record;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class LSMDAO implements DAO{

    private MemoryTable memoryTable;

    LSMDAO(long maxSize) {
        memoryTable = new MemoryTable(maxSize);
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull ByteBuffer from) throws IOException {
        return null;
    }

    @NotNull
    @Override
    public Iterator<Record> range(@NotNull ByteBuffer from, @Nullable ByteBuffer to) throws IOException {
        return null;
    }

    @NotNull
    @Override
    public ByteBuffer get(@NotNull ByteBuffer key) throws IOException, NoSuchElementException {
        return null;
    }

    @Override
    public void upsert(@NotNull ByteBuffer key, @NotNull ByteBuffer value) throws IOException {
        memoryTable.upsert(key, value);
    }

    @Override
    public void remove(@NotNull ByteBuffer key) throws IOException {
        memoryTable.remove(key);
    }

    @Override
    public void close() throws IOException {

    }
}

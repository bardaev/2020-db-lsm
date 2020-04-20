package ru.mail.polis;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;

public class ImplDAO implements DAO {

    private final SortedMap<ByteBuffer, Record> map = new TreeMap<>();

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) throws IOException {
        final Iterator<Record> iterator = map.tailMap(from).values().iterator();
        return iterator;
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws IOException {
        map.put(key, Record.of(key, value));
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        map.remove(key);
    }

    @Override
    public void close() throws IOException {
        map.clear();
    }
}

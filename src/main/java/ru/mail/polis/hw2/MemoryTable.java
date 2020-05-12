package ru.mail.polis.hw2;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;

public class MemoryTable implements Table {

    private final SortedMap<ByteBuffer, Value> map = new TreeMap<>();
    private long size;

    @NotNull
    @Override
    public Iterator<Cell> iterator(@NotNull final ByteBuffer from) {
        return Iterators.transform(
                map.tailMap(from).entrySet().iterator(),
                entry -> new Cell(entry.getKey(), entry.getValue())
        );
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) {
        map.put(key, new Value(System.currentTimeMillis(), value));
        size += key.remaining() + value.remaining() + Long.BYTES;
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) {
        if (map.containsKey(key)) {
            if (!map.get(key).isTombstone()) {
                size = size - map.get(key).getData().remaining();
            }
        } else {
            size += key.remaining() + Long.BYTES;
        }
        map.put(key, new Value(System.currentTimeMillis()));
    }

    public int getSize() {
        return map.size();
    }

    public long getSizeBytes() {
        return size;
    }

}

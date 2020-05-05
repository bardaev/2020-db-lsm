package ru.mail.polis.hw2;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;

public class MemoryTable implements Table {

    private final SortedMap<ByteBuffer, Value> map;
    private final long maxSize;

    MemoryTable(long maxSize) {
        this.maxSize = maxSize;
        map = new TreeMap<>();
    }

    @Override
    public void upsert(@NotNull ByteBuffer k, @NotNull ByteBuffer v) {
        map.put(k, new Value(v, false));
    }

    @Override
    public void remove(@NotNull ByteBuffer k) {
        map.put(k, new Value(null, true));
    }

    @NotNull
    @Override
    public Iterator<Cell> iterator(@NotNull ByteBuffer from) {
        return map.tailMap(from).entrySet().stream().map(map -> Cell.of(map.getKey(), map.getValue())).iterator();

    }


}

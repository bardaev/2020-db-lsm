package ru.mail.polis.hw2;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.DAO;
import ru.mail.polis.Iters;
import ru.mail.polis.Record;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class LSMDAO implements DAO {

    private static final Logger log = LoggerFactory.getLogger(LSMDAO.class);

    private static final String SUFFIX = ".dat";
    private static final String TEMP = ".tmp";


    @NotNull
    private final File storage;
    private final long flushTheshold;

    // data
    private MemoryTable memoryTable;
    private final NavigableMap<Integer, SSTable> ssTableMap;

    // state
    private int generation = 0;

    public LSMDAO(@NotNull final File storage, final long flushThreshold) throws IOException {
        assert flushThreshold > 0L;

        this.storage = storage;
        this.flushTheshold = flushThreshold;

        this.memoryTable = new MemoryTable();
        this.ssTableMap = new TreeMap<>();

        try(final Stream<Path> files = Files.list(storage.toPath())) {
            files
                    .filter(path -> path.toString().endsWith(SUFFIX))
                    .forEach(f -> {
                        try {
                            final String name = f.getFileName().toString();
                            final int generation = Integer.parseInt(name.substring(0, name.indexOf(SUFFIX)));
                            this.generation = Math.max(this.generation, generation);
                            ssTableMap.put(generation, new SSTable(f.toFile()));
                        } catch (IOException e) {
                            log.info("bad file");
                        } catch (NumberFormatException n) {
                            log.info("bad name");
                        }
                    });
        }
        this.generation++;
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull ByteBuffer from) throws IOException {
        final List<Iterator<Cell>> iters = new ArrayList<>(ssTableMap.size() + 1);
        iters.add(memoryTable.iterator(from));
        ssTableMap.descendingMap().values().forEach(t -> iters.add(t.iterator(from)));
        final Iterator<Cell> merge = Iterators.mergeSorted(iters, Cell.COMPARATOR);
        final Iterator<Cell> fresh = Iters.collapseEquals(merge, Cell::getKey);
        final Iterator<Cell> alive = Iterators.filter(fresh, e -> !e.getValue().isTombstone());
        return Iterators.transform(alive, e -> Record.of(e.getKey(), e.getValue().getData()));
    }

    @Override
    public void upsert(@NotNull ByteBuffer key, @NotNull ByteBuffer value) throws IOException {
        if (memoryTable.getSizeBytes() >= flushTheshold) flush();
        memoryTable.upsert(key, value);
    }

    @Override
    public void remove(@NotNull ByteBuffer key) throws IOException {
        if (memoryTable.getSizeBytes() >= flushTheshold) flush();
        memoryTable.remove(key);
    }

    private void flush() throws IOException {

        // dump memTable
        final File file = new File(storage, generation + TEMP);
        SSTable.serialize(file, memoryTable.iterator(ByteBuffer.allocate(0)));
        final File dst = new File(storage, generation + SUFFIX);
        Files.move(file.toPath(), dst.toPath(), StandardCopyOption.ATOMIC_MOVE);

        memoryTable = new MemoryTable();
        ssTableMap.put(generation, new SSTable(dst));
        generation++;
    }

    @Override
    public void close() throws IOException {
        if (memoryTable.getSize() > 0) flush();
    }
}

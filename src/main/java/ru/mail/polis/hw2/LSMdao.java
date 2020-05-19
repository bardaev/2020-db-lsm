package ru.mail.polis.hw2;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;
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
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

public class LSMdao implements DAO {

    private static final Logger log = LoggerFactory.getLogger(LSMdao.class);

    private static final String SUFFIX = ".dat";
    private static final String TEMP = ".tmp";

    @NotNull
    private final File storage;
    private final long flushTheshold;

    // data
    private MemoryTable memoryTable;
    private NavigableMap<Integer, SSTable> ssTableMap;

    // state
    private int generation;

    /**
     * Implementation {@link Table}.
     *
     * @param storage - storage
     * @param flushThreshold - flush when memTable is full
     */
    public LSMdao(@NotNull final File storage, final long flushThreshold) throws IOException {
        assert flushThreshold > 0L;

        this.storage = storage;
        this.flushTheshold = flushThreshold;

        this.memoryTable = new MemoryTable();
        this.ssTableMap = new TreeMap<>();

        try (Stream<Path> files = Files.list(storage.toPath())) {
            files
                    .filter(path -> path.toString().endsWith(SUFFIX))
                    .forEach(f -> {
                        try {
                            final String name = f.getFileName().toString();
                            final int gen = Integer.parseInt(name.substring(0, name.indexOf(SUFFIX)));
                            this.generation = Math.max(this.generation, gen);
                            ssTableMap.put(gen, new SSTable(f.toFile()));
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
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) {
        final Iterator<Cell> alive = Iterators.filter(cellIterator(from),
                cell -> !requireNonNull(cell).getValue().isTombstone());
        return Iterators.transform(alive, cell -> Record.of(requireNonNull(cell).getKey(), cell.getValue().getData()));
    }

    private Iterator<Cell> cellIterator(@NotNull final ByteBuffer from) {
        final List<Iterator<Cell>> iterators = new ArrayList<>(ssTableMap.size() + 1);
        iterators.add(memoryTable.iterator(from));
        ssTableMap.descendingMap().values().forEach(table -> iterators.add(table.iterator(from)));
        final Iterator<Cell> merged = Iterators.mergeSorted(iterators, Cell.COMPARATOR);
        return Iters.collapseEquals(merged, Cell::getKey);
    }

    @Override
    public void compact() throws IOException {
        final Iterator<Cell> iterator = cellIterator(ByteBuffer.allocate(0));
        final File tmp = new File(storage, generation + TEMP);
        SSTable.serialize(tmp, iterator);
        for (int i = 0; i < generation; i++) {
            try {
                Files.delete(new File(storage, i + SUFFIX).toPath());
            } catch (IOException e) {
                log.warn("Can not delete file");
            }

        }
        generation = 0;
        final File file = new File(storage, generation + SUFFIX);
        Files.move(tmp.toPath(), file.toPath(), StandardCopyOption.ATOMIC_MOVE);
        ssTableMap = new TreeMap<>();

        memoryTable = new MemoryTable();
        ssTableMap.put(generation, new SSTable(file));
        generation++;
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
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws IOException {
        if (memoryTable.getSizeBytes() >= flushTheshold) flush();
        memoryTable.upsert(key, value);
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        if (memoryTable.getSizeBytes() >= flushTheshold) flush();
        memoryTable.remove(key);
    }

    @Override
    public void close() throws IOException {
        if (memoryTable.getSize() > 0) flush();
        for (final Map.Entry<Integer, SSTable> el: ssTableMap.entrySet()) {
            el.getValue().close();
        }
    }
}

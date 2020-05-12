package ru.mail.polis.hw2;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class SSTable implements Table {

    private static final Logger log = LoggerFactory.getLogger(SSTable.class);

    private final FileChannel fileChannel;
    private final int count;
    private final int size;

    SSTable(@NotNull final File file) throws IOException {
        fileChannel = FileChannel.open(file.toPath(), StandardOpenOption.READ);
        final int fSize = (int) fileChannel.size() - Integer.BYTES;
        final ByteBuffer cellCount = ByteBuffer.allocate(Integer.BYTES);
        fileChannel.read(cellCount, fSize);
        this.count = cellCount.rewind().getInt();
        this.size = fSize - count * Integer.BYTES;
    }

    @NotNull
    @Override
    public Iterator<Cell> iterator(@NotNull final ByteBuffer from) {
        return new Iterator<>() {
            int pos = getPositionKey(from);

            @Override
            public boolean hasNext() {
                return pos < count;
            }

            @Override
            public Cell next() {
                if (!hasNext()) try {
                    throw new NoSuchFieldException();
                } catch (NoSuchFieldException e) {
                    log.info("NoSuchFieldException");
                }
                return getCell(pos++);
            }
        };
    }

    /**
     * serialize table to file.
     *
     * @param file - file
     * @param iterator - iterator
     */
    public static void serialize(final File file, final Iterator<Cell> iterator) throws IOException {

        final List<Integer> offsets = new ArrayList<>();
        int offset = 0;

        try (FileChannel fileSerialize = new FileOutputStream(file).getChannel()) {
            while (iterator.hasNext()) {
                final Cell cell = iterator.next();
                final ByteBuffer k = cell.getKey();
                offsets.add(offset);
                offset = offset + k.remaining() + Long.BYTES + Integer.BYTES;
                fileSerialize.write(ByteBuffer.allocate(Integer.BYTES).putInt(k.remaining()).rewind());
                fileSerialize.write(k);

                if (cell.getValue().isTombstone()) {
                    fileSerialize.write(ByteBuffer.allocate(Long.BYTES)
                            .putLong(-cell.getValue().getTimestamp()).rewind());
                } else {
                    fileSerialize.write(ByteBuffer.allocate(Long.BYTES)
                            .putLong(cell.getValue().getTimestamp()).rewind());
                    final ByteBuffer data = cell.getValue().getData();
                    offset += data.remaining();
                    fileSerialize.write(data);
                }
            }

            final int count = offsets.size();
            for (final Integer offs : offsets) {
                fileSerialize.write(ByteBuffer.allocate(Integer.BYTES).putInt(offs).rewind());
            }

            fileSerialize.write(ByteBuffer.allocate(Integer.BYTES).putInt(count).rewind());
        }
    }

    private int getPositionKey(final ByteBuffer key) {
        int low = 0;
        int high = count - 1;
        while (low <= high) {
            final int mid = low + (high - low) / 2;
            int k;
            try {
                k = getKey(mid).compareTo(key);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            if (k < 0) {
                low = mid + 1;
            } else if (k > 0) {
                high = mid - 1;
            } else {
                return mid;
            }
        }
        return low;
    }

    private Cell getCell(final int pos) {
        try {

            int offset = getOffset(pos);

            final ByteBuffer keyLength = ByteBuffer.allocate(Integer.BYTES);
            fileChannel.read(keyLength, offset);

            offset += Integer.BYTES;
            final int keySize = keyLength.rewind().getInt();
            final ByteBuffer key = ByteBuffer.allocate(keySize);

            fileChannel.read(key, offset);
            offset += keySize;
            final ByteBuffer version = ByteBuffer.allocate(Long.BYTES);
            fileChannel.read(version, offset);
            final long v = version.rewind().getLong();

            if (v < 0) {
                return new Cell(key.rewind(), new Value(-v));
            } else {
                offset += Long.BYTES;
                final int dataSize;
                if (pos == this.count - 1) {
                    dataSize = this.size - offset;
                } else {
                    dataSize = getOffset(pos + 1) - offset;
                }
                final ByteBuffer data = ByteBuffer.allocate(dataSize);
                fileChannel.read(data, offset);
                return new Cell(key.rewind(), new Value(v, data.rewind()));
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private ByteBuffer getKey(final int pos) throws IOException {

        final int offset = getOffset(pos);
        final ByteBuffer keyLength = ByteBuffer.allocate(Integer.BYTES);
        
        fileChannel.read(keyLength, offset);
        final int keySize = keyLength.rewind().getInt();
        final ByteBuffer key = ByteBuffer.allocate(keySize);

        fileChannel.read(key, offset + Integer.BYTES);
        return key.rewind();
    }

    private int getOffset(final int pos) throws IOException {
        final ByteBuffer offset = ByteBuffer.allocate(Integer.BYTES);
        fileChannel.read(offset, pos * Integer.BYTES + size);
        return offset.rewind().getInt();
    }

    public void close() throws IOException {
        fileChannel.close();
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) {
        throw new UnsupportedOperationException("Immutable");
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) {
        throw new UnsupportedOperationException("Immutable");
    }
}

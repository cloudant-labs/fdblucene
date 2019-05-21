package com.cloudant.fdblucene;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.jcs.JCS;
import org.apache.commons.jcs.access.GroupCacheAccess;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockObtainFailedException;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.TransactionContext;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;

/**
 * A concrete implementation of {@link Directory} that reads and writes all
 * index data into a FoundationDB {@link Database}.
 *
 */
public final class FDBDirectory extends Directory {

    private static class FileMetaData {

        private final Tuple asTuple;

        public FileMetaData(final long fileNumber, final long fileLength) {
            this.asTuple = Tuple.from(fileNumber, fileLength);
        }

        public FileMetaData(final Tuple tuple) {
            if (tuple.size() != 2) {
                throw new IllegalArgumentException(tuple + " is not a file metadata tuple");
            }
            this.asTuple = tuple;
        }

        public FileMetaData(final byte[] bytes) {
            this(Tuple.fromBytes(bytes));
        }

        public long getFileNumber() {
            return asTuple.getLong(0);
        }

        public long getFileLength() {
            return asTuple.getLong(1);
        }

        public FileMetaData setFileLength(final long fileLength) {
            return new FileMetaData(getFileNumber(), fileLength);
        }

        public byte[] pack() {
            return asTuple.pack();
        }

    }

    /**
     * Opens a Directory (or creates an empty one if there is no existing directory)
     * at the provided {@code path}.
     *
     * @param txc  The {@link TransactionContext} that will be used for all
     *             transactions. This is typically a {@link Database}.
     * @param path The (virtual) path where this directory is located. This option
     *             is provided for compatibility with the Lucene test framework. No
     *             data will be written to this path of the filesystem.
     * @return an instance of FDBDirectory
     */
    public static FDBDirectory open(final TransactionContext txc, final Path path) {
        return open(txc, path, FDBUtil.DEFAULT_PAGE_SIZE, FDBUtil.DEFAULT_TXN_SIZE);
    }

    /**
     * Opens a Directory (or creates an empty one if there is no existing directory)
     * at the provided {@code path}.
     *
     * @param txc      The {@link TransactionContext} that will be used for all
     *                 transactions. This is typically a {@link Database}.
     * @param path     The (virtual) path where this directory is located. This
     *                 option is provided for compatibility with the Lucene test
     *                 framework. No data will be written to this path of the
     *                 filesystem.
     * @param pageSize The size of the value stored in FoundationDB. Must be less
     *                 that {@code txnSize}. This value is ignored if the directory
     *                 already exists.
     * @param txnSize  The maximum size of the transaction FDBDirectory will make
     *                 when writing to FoundationDB. Must be at least as large as
     *                 {@code pageSize}.
     * @return an instance of FDBDirectory
     * @throws IllegalArgumentException if txnSize is smaller than pageSize.
     */
    public static FDBDirectory open(
            final TransactionContext txc,
            final Path path,
            final int pageSize,
            final int txnSize) {
        final DirectoryLayer dirLayer = DirectoryLayer.getDefault();
        final DirectorySubspace dir = dirLayer.createOrOpen(txc, pathAsList(path)).join();
        return open(txc, dir, pageSize, txnSize);
    }

    /**
     * Opens a Directory (or creates an empty one if there is no existing directory)
     * at the provided {@code path}.
     *
     * @param txc      The {@link TransactionContext} that will be used for all
     *                 transactions. This is typically a {@link Database}.
     * @param subspace The {@link Subspace} to create all key-value entries under.
     *                 This is useful if using Lucene indexes in a wider context.
     * @param pageSize The size of the value stored in FoundationDB. Must be less
     *                 that {@code txnSize}. This value is ignored if the directory
     *                 already exists.
     * @param txnSize  The maximum size of the transaction FDBDirectory will make
     *                 when writing to FoundationDB. Must be at least as large as
     *                 {@code pageSize}.
     * @return an instance of FDBDirectory
     * @throws IllegalArgumentException if txnSize is smaller than pageSize.
     */
    public static FDBDirectory open(
            final TransactionContext txc,
            final Subspace subspace,
            final int pageSize,
            final int txnSize) {
        return new FDBDirectory(txc, subspace, pageSize, txnSize);
    }

    private static List<String> pathAsList(final Path path) {
        final List<String> result = new ArrayList<String>();
        for (final Path p : path) {
            result.add(p.toString());
        }
        return result;
    }

    private final TransactionContext txc;
    private final Subspace subspace;
    private boolean closed;
    private final int pageSize;
    private final int txnSize;

    private final UUID uuid;
    private final GroupCacheAccess<Long, byte[]> pageCache;

    private FDBDirectory(final TransactionContext txc, final Subspace subspace, final int pageSize, final int txnSize) {
        this.txc = txc;
        this.subspace = subspace;
        this.closed = false;
        this.pageSize = getOrSetPageSize(txc, subspace, pageSize);
        this.txnSize = txnSize;

        if (this.txnSize < this.pageSize) {
            throw new IllegalArgumentException("txnSize cannot be smaller than pageSize");
        }

        this.uuid = UUID.randomUUID();
        this.pageCache = JCS.getGroupCacheInstance(uuid.toString());
    }

    /**
     * Removes all data related to this directory.
     */
    public void delete() {
        txc.run(txn -> {
            txn.clear(subspace.range());
            return null;
        });
    }

    @Override
    public void close() throws IOException {
        pageCache.clear();
        closed = true;
    }

    /**
     * Creates a new {@link FDBIndexOutput} instance.
     *
     * @param name    the name of the output file.
     * @param context this parameter is ignored. It is safe to pass {@code null} in
     *                tests.
     */
    @Override
    public IndexOutput createOutput(final String name, final IOContext context) throws IOException {
        if (closed) {
            throw new AlreadyClosedException(this + " is closed");
        }

        final byte[] key = metaKey(name);

        final long fileNumber = txc.run(txn -> {
            txn.options().setTransactionLoggingEnable(String.format("createOutput(%s)", name));
            final byte[] value = txn.get(key).join();
            if (value != null) {
                return -1L;
            }

            final long result = getAndIncrement(txn, "_fn");
            txn.set(key, new FileMetaData(result, 0L).pack());
            return result;
        });

        if (fileNumber == -1L) {
            throw new FileAlreadyExistsException(name + " already exists.");
        }

        final String resourceDescription = String.format("FDBIndexOutput(name=%s,number=%d)", name, fileNumber);
        return new FDBIndexOutput(this, resourceDescription, name, txc, fileSubspace(fileNumber), pageSize, txnSize);
    }

    /**
     * Creates a new {@link FDBIndexOutput} instance.
     *
     * @param context this parameter is ignored. It is safe to pass {@code null} in
     *                tests.
     */
    @Override
    public IndexOutput createTempOutput(final String prefix, final String suffix, final IOContext context)
            throws IOException {
        while (true) {
            final long number = FDBUtil.RANDOM.nextInt();
            final String name = String
                    .format("%s_%s_%s.tmp", prefix, suffix, Long.toString(number, Character.MAX_RADIX));
            try {
                return createOutput(name, context);
            } catch (final FileAlreadyExistsException e) {
                // Retry.
            }
        }
    }

    @Override
    public void deleteFile(final String name) throws IOException {
        final boolean deleted = txc.run(txn -> {
            txn.options().setTransactionLoggingEnable(String.format("deleteFile(%s)", name));
            final long fileNumber = fileNumber(txn, name);
            if (fileNumber != -1L) {
                txn.clear(metaKey(name));
                txn.clear(subspace.get(fileNumber).range());
                return true;
            }
            return false;
        });

        if (!deleted) {
            throw new FileNotFoundException(name + " does not exist");
        }

        pageCache.invalidateGroup(name);
    }

    @Override
    public long fileLength(final String name) throws IOException {
        final FileMetaData meta = meta(txc, name);

        if (meta == null) {
            throw new FileNotFoundException(name + " does not exist.");
        }

        return meta.getFileLength();
    }

    void setFileLength(final TransactionContext txc, final String name, final long length) {
        final byte[] metaKey = metaKey(name);
        txc.run(txn -> {
            final byte[] value = txn.get(metaKey).join();
            final FileMetaData meta = new FileMetaData(value).setFileLength(length);
            txn.set(metaKey, meta.pack());
            return null;
        });
    }

    @Override
    public String[] listAll() throws IOException {
        final Range metaRange = metaRange();
        final List<KeyValue> keyvalues = txc.read(txn -> {
            txn.options().setTransactionLoggingEnable("listAll");
            return txn.getRange(metaRange).asList().join();
        });

        final String[] result = new String[keyvalues.size()];
        for (int i = 0; i < keyvalues.size(); i++) {
            final byte[] key = keyvalues.get(i).getKey();
            final Tuple tuple = subspace.unpack(key);
            result[i] = tuple.getString(1);
        }
        return result;
    }

    @Override
    public Lock obtainLock(final String name) throws IOException {
        try {
            createOutput(name, null).close();
            return new FDBLock(this, name);
        } catch (FileAlreadyExistsException e) {
            throw new LockObtainFailedException("Lock for " + name + " already obtained.", e);
        }
    }

    /**
     * Creates a new {@link FDBIndexInput} instance.
     *
     * @param context this parameter is ignored. It is safe to pass {@code null} in
     *                tests.
     */
    @Override
    public IndexInput openInput(final String name, final IOContext context) throws IOException {
        if (closed) {
            throw new AlreadyClosedException(this + " is closed");
        }

        final FileMetaData meta = meta(txc, name);

        if (meta == null) {
            throw new FileNotFoundException(name + " does not exist.");
        }

        final String resourceDescription = String
                .format("FDBIndexInput(name=%s,number=%d)", name, meta.getFileNumber());
        return new FDBIndexInput(resourceDescription, txc, fileSubspace(meta.getFileNumber()), name, 0L,
                meta.getFileLength(), pageSize, pageCache);
    }

    /**
     * Atomically renames a file in constant time.
     */
    @Override
    public void rename(final String source, final String dest) throws IOException {
        final byte[] sourceKey = metaKey(source);
        final byte[] destKey = metaKey(dest);

        txc.run(txn -> {
            txn.options().setTransactionLoggingEnable(String.format("rename(%s,%s)", source, dest));
            final FileMetaData meta = meta(txn, source);
            txn.clear(sourceKey);
            txn.set(destKey, meta.pack());
            return null;
        });
    }

    /**
     * this method's implementation is empty.
     */
    @Override
    public void sync(final Collection<String> names) throws IOException {
        // intentionally empty
    }

    /**
     * this method's implementation is empty.
     */
    @Override
    public void syncMetaData() throws IOException {
        // intentionally empty
    }

    @Override
    public Set<String> getPendingDeletions() throws IOException {
        return Collections.emptySet();
    }

    @Override
    public String toString() {
        return String.format("FDBDirectory(subspace=%s,uuid=%s)", subspace, uuid);
    }

    private Subspace fileSubspace(final long fileNumber) {
        return subspace.get(fileNumber);
    }

    private long getAndIncrement(final TransactionContext txc, final String counterName) {
        final byte[] key = subspace.pack(Tuple.from("_counter", counterName));
        return txc.run(txn -> {
            final byte[] value = txn.get(key).join();
            if (value == null) {
                txn.set(key, FDBUtil.encodeLong(1L));
                return 0L;
            } else {
                final long result = FDBUtil.decodeLong(value);
                txn.set(key, FDBUtil.encodeLong(result + 1L));
                return result;
            }
        });
    }

    private int getOrSetPageSize(final TransactionContext txc, final Subspace subspace, final int pageSize) {
        final byte[] key = subspace.pack(Tuple.from("_pagesize"));
        return txc.run(txn -> {
            final byte[] pageSizeInFDB = txn.get(key).join();
            if (pageSizeInFDB == null) {
                txn.set(key, FDBUtil.encodeInt(pageSize));
                return pageSize;
            } else {
                return FDBUtil.decodeInt(pageSizeInFDB);
            }
        });
    }

    private long fileNumber(final TransactionContext txc, final String name) {
        final FileMetaData meta = meta(txc, name);
        if (meta == null) {
            return -1L;
        }
        return meta.getFileNumber();
    }

    private FileMetaData meta(final TransactionContext txc, final String name) {
        final byte[] key = metaKey(name);
        final byte[] result = txc.read(txn -> {
            return txn.get(key).join();
        });

        if (result == null) {
            return null;
        }

        return new FileMetaData(Tuple.fromBytes(result));
    }

    private byte[] metaKey(final String name) {
        return subspace.pack(metaTuple(name));
    }

    private Range metaRange() {
        return subspace.range(Tuple.from("_meta"));
    }

    private Tuple metaTuple(final String name) {
        return Tuple.from("_meta", name);
    }

}

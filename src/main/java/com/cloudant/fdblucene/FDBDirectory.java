package com.cloudant.fdblucene;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletionException;

import org.apache.commons.jcs.JCS;
import org.apache.commons.jcs.access.GroupCacheAccess;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.TransactionContext;
import com.apple.foundationdb.directory.DirectoryAlreadyExistsException;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.directory.NoSuchDirectoryException;

/**
 * A concrete implementation of {@link Directory} that reads and writes all
 * index data into a FoundationDB {@link Database}.
 *
 */
public final class FDBDirectory extends Directory {

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
     * @param dir      The {@link DirectorySubspace} to create all key-value entries
     *                 under. This is useful if using Lucene indexes in a wider
     *                 context.
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
            final DirectorySubspace dir,
            final int pageSize,
            final int txnSize) {
        return new FDBDirectory(txc, dir, pageSize, txnSize);
    }

    private static List<String> pathAsList(final Path path) {
        final List<String> result = new ArrayList<String>();
        for (final Path p : path) {
            result.add(p.toString());
        }
        return result;
    }

    private final TransactionContext txc;
    private final DirectorySubspace dir;
    private boolean closed;
    private final int pageSize;
    private final int txnSize;

    private final UUID uuid;
    private final GroupCacheAccess<Long, byte[]> pageCache;

    private FDBDirectory(final TransactionContext txc, final DirectorySubspace dir, final int pageSize,
            final int txnSize) {
        this.txc = txc;
        this.dir = dir;
        this.closed = false;
        this.pageSize = getOrSetPageSize(txc, dir, pageSize);
        this.txnSize = txnSize;

        if (this.txnSize < this.pageSize) {
            throw new IllegalArgumentException("txnSize cannot be smaller than pageSize");
        }

        this.uuid = UUID.randomUUID();
        this.pageCache = JCS.getGroupCacheInstance(uuid.toString());
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
            throw new AlreadyClosedException(dir + " is closed");
        }

        try {
            return txc.run(txn -> {
                txn.options().setTransactionLoggingEnable(String.format("createOutput,%s", name));
                final DirectorySubspace result = dir.create(txn, asList(name)).join();
                txn.set(result.pack("length"), FDBUtil.encodeLong(0L));
                final String resourceDescription = "FDBIndexOutput(subdir=\"" + result + "\")";
                return new FDBIndexOutput(resourceDescription, name, txc, result, pageSize, txnSize);

            });
        } catch (final CompletionException e) {
            if (e.getCause() instanceof DirectoryAlreadyExistsException) {
                throw new FileAlreadyExistsException(name + " already exists.");
            }
            throw new IOException(e);
        }
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
        if (closed) {
            throw new AlreadyClosedException(dir + " is closed");
        }

        final DirectorySubspace subdir = txc.run(txn -> {
            txn.options().setTransactionLoggingEnable(String.format("createTempOutput,%s,%s", prefix, suffix));
            while (true) {
                final List<String> subpath = asList(
                        String.format("%s%x%s.tmp", prefix, FDBUtil.RANDOM.nextInt(), suffix));
                if (!dir.exists(txn, subpath).join()) {
                    final DirectorySubspace result = dir.create(txn, subpath).join();
                    txn.set(result.pack("length"), FDBUtil.encodeLong(0L));
                    return result;
                }
            }
        });
        final List<String> path = subdir.getPath();
        final String name = path.get(path.size() - 1);
        final String resourceDescription = "FDBIndexOutput(subdir=\"" + subdir + "\")";
        return new FDBIndexOutput(resourceDescription, name, txc, subdir, pageSize, txnSize);
    }

    @Override
    public void deleteFile(final String name) throws IOException {
        final boolean deleted = dir.removeIfExists(txc, asList(name)).join();
        if (deleted) {
            pageCache.invalidateGroup(name);
        } else {
            throw new FileNotFoundException(name + " does not exist");
        }
    }

    @Override
    public long fileLength(final String name) throws IOException {
        try {
            return txc.run(txn -> {
                txn.options().setTransactionLoggingEnable(String.format("fileLength,%s", name));
                final DirectorySubspace subdir = dir.open(txn, asList(name)).join();
                return FDBUtil.decodeLong(txn.get(subdir.pack("length")).join());
            });
        } catch (final CompletionException e) {
            if (e.getCause() instanceof NoSuchDirectoryException) {
                throw new FileNotFoundException(name + " does not exist.");
            }
            throw new IOException(e);
        }
    }

    @Override
    public String[] listAll() throws IOException {
        final List<String> result = txc.run(txn -> {
            txn.options().setTransactionLoggingEnable("listAll");
            return dir.list(txn).join();
        });
        return result.toArray(new String[0]);
    }

    @Override
    public Lock obtainLock(final String name) throws IOException {
        return FDBLock.obtain(txc, dir, name);
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
            throw new AlreadyClosedException(dir + " is closed");
        }

        try {
            return txc.run(txn -> {
                txn.options().setTransactionLoggingEnable(String.format("openInput,%s", name));
                final DirectorySubspace subdir = dir.open(txn, asList(name)).join();
                final long length = FDBUtil.decodeLong(txn.get(subdir.pack("length")).join());
                final String resourceDescription = "FDBIndexOutput(subdir=\"" + subdir + "\")";
                return new FDBIndexInput(resourceDescription, txc, subdir, name, 0L, length, pageSize, pageCache);
            });
        } catch (final CompletionException e) {
            if (e.getCause() instanceof NoSuchDirectoryException) {
                throw new FileNotFoundException(name + " does not exist.");
            }
            throw new IOException(e);
        }
    }

    /**
     * Atomically renames a file using {@link DirectorySubspace#move(TransactionContext, List, List)} method.
     */
    @Override
    public void rename(final String source, final String dest) throws IOException {
        txc.run(txn -> {
            txn.options().setTransactionLoggingEnable(String.format("move,%s,%s", source, dest));
            dir.move(txn, asList(source), asList(dest)).join();
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

    private List<String> asList(final String name) {
        return Collections.singletonList(name);
    }

    private int getOrSetPageSize(final TransactionContext txc, final DirectorySubspace dir, final int pageSize) {
        return txc.run(txn -> {
            byte[] pageSizeInFDB = txn.get(dir.pack("pagesize")).join();
            if (pageSizeInFDB == null) {
                txn.set(dir.pack("pagesize"), FDBUtil.encodeInt(pageSize));
                return pageSize;
            } else {
                return FDBUtil.decodeInt(pageSizeInFDB);
            }
        });
    }

    private String toString(final DirectorySubspace dir) {
        return String.join("/", dir.getPath());
    }

    @Override
    public String toString() {
        return String.format("FDBDirectory(path=/%s,uuid=%s)", toString(dir), uuid);
    }

}

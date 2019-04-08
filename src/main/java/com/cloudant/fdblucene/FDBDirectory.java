package com.cloudant.fdblucene;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionException;

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

public final class FDBDirectory extends Directory {

    public static FDBDirectory open(final Database db, final Path path) {
        final DirectoryLayer dirLayer = DirectoryLayer.getDefault();
        return new FDBDirectory(db, dirLayer.createOrOpen(db, pathAsList(path)).join());
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

    private FDBDirectory(final TransactionContext txc, final DirectorySubspace dir) {
        this.txc = txc;
        this.dir = dir;
        this.closed = false;
    }

    @Override
    public void close() throws IOException {
        closed = true;
    }

    @Override
    public IndexOutput createOutput(final String name, final IOContext context) throws IOException {
        if (closed) {
            throw new AlreadyClosedException(dir + " is closed");
        }

        try {
            return txc.run(txn -> {
                final DirectorySubspace result = dir.create(txn, asList(name)).join();
                txn.set(result.pack("length"), FDBUtil.encodeLong(0L));
                final String resourceDescription = "FDBIndexOutput(subdir=\"" + result + "\")";
                return new FDBIndexOutput(resourceDescription, name, txc, result);

            });
        } catch (final CompletionException e) {
            if (e.getCause() instanceof DirectoryAlreadyExistsException) {
                throw new FileAlreadyExistsException(name + " already exists.");
            }
            throw new IOException(e);
        }
    }

    @Override
    public IndexOutput createTempOutput(final String prefix, final String suffix, final IOContext context)
            throws IOException {
        if (closed) {
            throw new AlreadyClosedException(dir + " is closed");
        }

        final DirectorySubspace subdir = txc.run(txn -> {
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
        return new FDBIndexOutput(resourceDescription, name, txc, subdir);
    }

    @Override
    public void deleteFile(final String name) throws IOException {
        final boolean deleted = dir.removeIfExists(txc, asList(name)).join();
        if (!deleted) {
            throw new FileNotFoundException(name + " does not exist");
        }
    }

    @Override
    public long fileLength(final String name) throws IOException {
        try {
            return txc.run(txn -> {
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
        final List<String> result = dir.list(txc).join();
        return result.toArray(new String[0]);
    }

    @Override
    public Lock obtainLock(final String name) throws IOException {
        return FDBLock.obtain(txc, dir, name);
    }

    @Override
    public IndexInput openInput(final String name, final IOContext context) throws IOException {
        if (closed) {
            throw new AlreadyClosedException(dir + " is closed");
        }

        try {
            return txc.run(txn -> {
                final DirectorySubspace subdir = dir.open(txn, asList(name)).join();
                final long length = FDBUtil.decodeLong(txn.get(subdir.pack("length")).join());
                final String resourceDescription = "FDBIndexOutput(subdir=\"" + subdir + "\")";
                return new FDBIndexInput(resourceDescription, txc, subdir, 0L, length);
            });
        } catch (final CompletionException e) {
            if (e.getCause() instanceof NoSuchDirectoryException) {
                throw new FileNotFoundException(name + " does not exist.");
            }
            throw new IOException(e);
        }
    }

    @Override
    public void rename(final String source, final String dest) throws IOException {
        dir.move(txc, asList(source), asList(dest)).join();
    }

    @Override
    public void sync(final Collection<String> names) throws IOException {
        // intentionally empty
    }

    @Override
    public void syncMetaData() throws IOException {
        // intentionally empty
    }

    private List<String> asList(final String name) {
        return Collections.singletonList(name);
    }

}

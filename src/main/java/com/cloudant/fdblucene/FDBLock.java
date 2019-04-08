package com.cloudant.fdblucene;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.CompletionException;

import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.store.LockReleaseFailedException;

import com.apple.foundationdb.TransactionContext;
import com.apple.foundationdb.directory.DirectoryAlreadyExistsException;
import com.apple.foundationdb.directory.DirectorySubspace;

public final class FDBLock extends Lock {

    public static Lock obtain(final TransactionContext txc, final DirectorySubspace subdir, final String name) throws IOException {
        final DirectorySubspace lock;
        try {
            lock = subdir.create(txc, Collections.singletonList(name)).join();
        }  catch (final CompletionException e) {
            if (e.getCause() instanceof DirectoryAlreadyExistsException) {
                throw new LockObtainFailedException(name + " already acquired");
            }
            throw (e);
        }
        return new FDBLock(txc, lock, name);
    }

    private final TransactionContext txc;
    private final DirectorySubspace lock;
    private final String name;
    private boolean closed = false;

    private FDBLock(final TransactionContext txc, final DirectorySubspace lock, final String name) {
        this.txc = txc;
        this.lock = lock;
        this.name = name;
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }

        final boolean released = lock.removeIfExists(txc).join();
        if (!released) {
            throw new LockReleaseFailedException(name + " not released properly");
        }
        closed = true;
    }

    @Override
    public void ensureValid() throws IOException {
        if (closed) {
            throw new AlreadyClosedException(name + " already closed.");
        }

        final boolean valid = lock.exists(txc).join();
        if (!valid) {
            throw new IOException(name + " no longer valid");
        }
    }

    @Override
    public String toString() {
        return String.format("FDBLock(name=%s)", name);
    }

}

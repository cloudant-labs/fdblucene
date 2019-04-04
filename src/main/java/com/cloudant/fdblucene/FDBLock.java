package com.cloudant.fdblucene;

import java.io.IOException;
import java.util.Arrays;

import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.store.LockReleaseFailedException;

import com.apple.foundationdb.TransactionContext;

public final class FDBLock extends Lock {

    public static Lock obtain(final TransactionContext txc, final String name, final byte[] key) throws IOException {
        final byte[] value = txc.run(txn -> {
            if (txn.get(key).join() != null) {
                return null;
            }
            final byte[] v = randomValue();
            txn.set(key, v);
            return v;
        });
        if (value == null) {
            throw new LockObtainFailedException(name + " already acquired");
        }
        return new FDBLock(txc, name, key, value);
    }

    private static byte[] randomValue() {
        final byte[] result = new byte[32];
        FDBUtil.RANDOM.nextBytes(result);
        return result;
    }

    private final TransactionContext txc;
    private final String name;
    private final byte[] key;
    private final byte[] value;
    private boolean closed = false;

    private FDBLock(final TransactionContext txc, final String name, final byte[] key, final byte[] value) {
        this.txc = txc;
        this.name = name;
        this.key = key;
        this.value = value;
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }

        final boolean released = txc.run(txn -> {
            final byte[] value = txn.get(key).join();
            if (Arrays.equals(this.value, value)) {
                txn.clear(key);
                return true;
            }
            return false;
        });
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

        final boolean valid = txc.run(txn -> {
            final byte[] value = txn.get(key).join();
            if (value == null) {
                return false;
            }
            return Arrays.equals(this.value, value);
        });
        if (!valid) {
            throw new IOException(name + " no longer valid");
        }
    }

    @Override
    public String toString() {
        return String.format("FDBLock(name=%s)", name);
    }

}

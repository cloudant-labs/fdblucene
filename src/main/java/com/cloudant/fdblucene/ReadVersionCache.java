package com.cloudant.fdblucene;

import java.util.concurrent.TimeUnit;

import com.apple.foundationdb.Transaction;

final class ReadVersionCache {

    private static final long MAX_AGE = TimeUnit.NANOSECONDS.convert(4, TimeUnit.SECONDS);

    private long readVersion = -1L;

    private long readVersionAt;

    public ReadVersionCache() {
    }

    public void setReadVersion(final Transaction txn) {
        final long now = System.nanoTime();
        if (readVersion == -1L) {
            readVersion = txn.getReadVersion().join();
            readVersionAt = now;
            return;
        }
        final long readVersionAge = now - readVersionAt;
        if (readVersionAge > MAX_AGE) {
            readVersion = txn.getReadVersion().join();
            readVersionAt = now;
        } else {
            txn.setReadVersion(readVersion);
        }
    }

}

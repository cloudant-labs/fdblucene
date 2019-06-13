package com.cloudant.fdblucene;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.Lock;

final class FDBLock extends Lock {

    private final Directory dir;
    private final String name;
    private boolean closed = false;

    FDBLock(final Directory dir, final String name) {
        this.dir = dir;
        this.name = name;
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        dir.deleteFile(name);
        closed = true;
    }

    @Override
    public void ensureValid() throws IOException {
        if (closed) {
            throw new AlreadyClosedException(name + " already closed.");
        }

        try {
            dir.fileLength(name);
        } catch (final FileNotFoundException e) {
            throw new IOException(name + " no longer valid");
        }
    }

    @Override
    public String toString() {
        return String.format("FDBLock(name=%s)", name);
    }

}

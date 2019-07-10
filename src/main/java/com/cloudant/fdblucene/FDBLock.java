/*******************************************************************************
 * Copyright 2019 IBM Corporation
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package com.cloudant.fdblucene;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.util.UUID;

import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockObtainFailedException;

final class FDBLock extends Lock {

    private final Directory dir;
    private final UUID uuid;
    private final String name;
    private boolean invalid = false;
    private boolean closed = false;

    public static Lock obtain(final Directory dir, final UUID uuid, final String name) throws IOException {
        try {
            try (final IndexOutput output = dir.createOutput(name, null)) {
                output.writeLong(uuid.getMostSignificantBits());
                output.writeLong(uuid.getLeastSignificantBits());
            }
            return new FDBLock(dir, uuid, name);
        } catch (FileAlreadyExistsException e) {
            throw new LockObtainFailedException("Lock for " + name + " already obtained.", e);
        }
    }

    FDBLock(final Directory dir, final UUID uuid, final String name) {
        this.dir = dir;
        this.uuid = uuid;
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
        if (invalid) {
            throw new IOException(name + " no longer valid");
        }

        try (final IndexInput input = dir.openInput(name, null)) {
            final long msb = input.readLong();
            final long lsb = input.readLong();
            final UUID current = new UUID(msb, lsb);
            if (!this.uuid.equals(current)) {
                invalid = true;
                ensureValid();
            }
        } catch (final FileNotFoundException e) {
            invalid = true;
            ensureValid();
        }
    }

    @Override
    public String toString() {
        return String.format("FDBLock(name=%s)", name);
    }

}

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

import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockObtainFailedException;

final class FDBLock extends Lock {

    private final Directory dir;
    private final String name;
    private boolean closed = false;

    public static Lock obtain(final Directory dir, final String name) throws IOException {
        try {
            dir.createOutput(name, null).close();
            return new FDBLock(dir, name);
        } catch (FileAlreadyExistsException e) {
            throw new LockObtainFailedException("Lock for " + name + " already obtained.", e);
        }
    }

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

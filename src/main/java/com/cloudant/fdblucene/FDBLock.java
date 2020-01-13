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

import java.io.IOException;
import java.util.Arrays;
import java.util.UUID;

import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockObtainFailedException;

import com.cloudant.fdblucene.Utils;

import com.apple.foundationdb.TransactionContext;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;

final class FDBLock extends Lock {

    private final TransactionContext txc;
    private final byte[] uuid;
    private final String name;
    private final byte[] key;
    private boolean closed = false;

    public static Lock obtain(final TransactionContext txc, final Subspace subspace, final UUID uuid, final String name)
            throws IOException {
        final byte[] key = lockKey(subspace, name);
        final boolean obtained = txc.run(txn -> {
            Utils.trace(txn, "obtain(%s)", name);
            return txn.get(key).thenApply(value -> {
                if (value == null) {
                    txn.set(key, Utils.toBytes(uuid));
                    return true;
                }
                return false;
            }).join();
        });

        if (obtained) {
            return new FDBLock(txc, key, uuid, name);
        } else {
            throw new LockObtainFailedException("Lock for " + name + " already obtained.");
        }
    }

    FDBLock(final TransactionContext txc, final byte[] key, final UUID uuid, final String name) {
        this.txc = txc;
        this.uuid = Utils.toBytes(uuid);
        this.name = name;
        this.key = key;
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }

        try {
            txc.run(txn -> {
                Utils.trace(txn, "FDBLock.close(%s)", this.name);
                return txn.get(key).thenApply(value -> {
                    if (value != null && Arrays.equals(uuid, value)) {
                        txn.clear(key);
                        return true;
                    }
                    return false;
                }).join();
            });
        } finally {
            closed = true;
        }
    }

    @Override
    public void ensureValid() throws IOException {
        if (closed) {
            throw new AlreadyClosedException(name + " already closed.");
        }

        final boolean valid = txc.read(txn -> {
            Utils.trace(txn, "FDBLock.ensureValid(%s)", name);
            return txn.get(key).thenApply(value -> {
                return value != null && Arrays.equals(uuid, value);
            }).join();
        });

        if (!valid) {
            throw new AlreadyClosedException(name + " no longer valid.");
        }
    }

    public static void unlock(final TransactionContext txc, final Subspace subspace, final String name) {
        final byte[] key = lockKey(subspace, name);
        txc.run(txn -> {
            Utils.trace(txn, "FDBLock.unlock(%s)", name);
            txn.clear(key);
            return null;
        });
    }

    @Override
    public String toString() {
        return String.format("FDBLock(name=%s)", name);
    }

    private static byte[] lockKey(final Subspace subspace, final String name) {
        return subspace.pack(Tuple.from("_lock", name));
    }

}

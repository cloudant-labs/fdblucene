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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.security.SecureRandom;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.DatabaseOptions;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.Transaction;

final class FDBUtil {

    static {
        FDB.selectAPIVersion(600);
    }

    public static final Random RANDOM = new SecureRandom();

    static final int DEFAULT_PAGE_SIZE = 100_000;

    static final int DEFAULT_TXN_SIZE = 1_000_000;

    static int decodeInt(final byte[] v) {
        return (((v[0] & 0xff) << 24) | ((v[1] & 0xff) << 16) | ((v[2] & 0xff) << 8) | (v[3] & 0xff));
    }

    static long decodeLong(final byte[] v) {
        return (((long) (v[0] & 0xff) << 56) | ((long) (v[1] & 0xff) << 48) | ((long) (v[2] & 0xff) << 40)
                | ((long) (v[3] & 0xff) << 32) | ((long) (v[4] & 0xff) << 24) | ((long) (v[5] & 0xff) << 16)
                | ((long) (v[6] & 0xff) << 8) | (v[7] & 0xff));
    }

    static byte[] encodeInt(final int v) {
        final byte[] result = new byte[4];
        result[0] = (byte) (0xff & (v >> 24));
        result[1] = (byte) (0xff & (v >> 16));
        result[2] = (byte) (0xff & (v >> 8));
        result[3] = (byte) (0xff & v);
        return result;
    }

    static byte[] encodeLong(final long v) {
        final byte[] result = new byte[8];
        result[0] = (byte) (0xff & (v >> 56));
        result[1] = (byte) (0xff & (v >> 48));
        result[2] = (byte) (0xff & (v >> 40));
        result[3] = (byte) (0xff & (v >> 32));
        result[4] = (byte) (0xff & (v >> 24));
        result[5] = (byte) (0xff & (v >> 16));
        result[6] = (byte) (0xff & (v >> 8));
        result[7] = (byte) (0xff & v);
        return result;
    }

    static int posToOffset(final long pos, final int pageSize) {
        return (int) (pos % pageSize);
    }

    static long posToPage(final long pos, final int pageSize) {
        return pos / pageSize;
    }

    private static class TestDatabase implements Database {

        private final Process server;
        private final Database db;

        private TestDatabase(final Process server, final Database db) {
            this.server = server;
            this.db = db;
        }

        public Transaction createTransaction() {
            return db.createTransaction();
        }

        public Transaction createTransaction(Executor e) {
            return db.createTransaction(e);
        }

        public DatabaseOptions options() {
            return db.options();
        }

        public <T> T read(Function<? super ReadTransaction, T> retryable) {
            return db.read(retryable);
        }

        public Executor getExecutor() {
            return db.getExecutor();
        }

        public <T> T read(Function<? super ReadTransaction, T> retryable, Executor e) {
            return db.read(retryable, e);
        }

        public <T> CompletableFuture<T> readAsync(
                Function<? super ReadTransaction, ? extends CompletableFuture<T>> retryable) {
            return db.readAsync(retryable);
        }

        public <T> CompletableFuture<T> readAsync(
                Function<? super ReadTransaction, ? extends CompletableFuture<T>> retryable, Executor e) {
            return db.readAsync(retryable, e);
        }

        public <T> T run(Function<? super Transaction, T> retryable) {
            return db.run(retryable);
        }

        public <T> T run(Function<? super Transaction, T> retryable, Executor e) {
            return db.run(retryable, e);
        }

        public <T> CompletableFuture<T> runAsync(
                Function<? super Transaction, ? extends CompletableFuture<T>> retryable) {
            return db.runAsync(retryable);
        }

        public <T> CompletableFuture<T> runAsync(
                Function<? super Transaction, ? extends CompletableFuture<T>> retryable, Executor e) {
            return db.runAsync(retryable, e);
        }

        public void close() {
            db.close();
            server.destroy();
        }

    }

    static Database getTestDb(final boolean empty) throws IOException {
        final String userDir = System.getProperty("user.dir");
        final File fdbDir = new File(new File(userDir, "target"), ".fdblucene");
        fdbDir.mkdir();

        final int port = getAvailablePort();

        final File clusterFile = new File(fdbDir, "fdblucene.cluster");
        try (final PrintWriter writer = new PrintWriter(clusterFile)) {
            writer.format("fdblucene:fdblucene@127.0.0.1:%d\n", port);
        }

        final Process fdbServer = startTestDb(fdbDir, clusterFile, port);
        final Database result = FDB.instance().open(clusterFile.getAbsolutePath());
        if (empty) {
            clear(result);
        }
        return new TestDatabase(fdbServer, result);
    }

    static void clear(final Database db) {
        db.run(txn -> {
            txn.clear(new byte[0], new byte[] { (byte) 0xfe, (byte) 0xff, (byte) 0xff });
            return null;
        });
    }

    private static Process startTestDb(final File fdbDir, final File clusterFile, final int port)
            throws IOException {
        // Our own private fdbserver.
        ProcessBuilder builder = new ProcessBuilder(findFdbServerBin(), "-p", String.format("127.0.0.1:%d", port), "-C",
                clusterFile.getAbsolutePath(), "-d", fdbDir.getAbsolutePath(), "-L", fdbDir.getAbsolutePath());
        final Process fdbServer = builder.start();

        // Initialise the db
        builder = new ProcessBuilder(findFdbCliBin(), "-C", clusterFile.getAbsolutePath(), "--exec",
                "configure new single ssd");
        try {
            builder.start().waitFor();
        } catch (InterruptedException e) {
            // Ignored.
        }

        return fdbServer;
    }

    private static int getAvailablePort() throws IOException {
        try (final ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static String findFdbServerBin() {
        final String[] locations = new String[] { "/usr/sbin/fdbserver", "/usr/local/sbin/fdbserver",
                "/usr/local/libexec/fdbserver" };
        for (final String location : locations) {
            if (new File(location).isFile()) {
                return location;
            }
        }
        throw new Error("fdbserver not found.");
    }

    private static String findFdbCliBin() {
        final String[] locations = new String[] { "/usr/bin/fdbcli", "/usr/local/bin/fdbcli" };
        for (final String location : locations) {
            if (new File(location).isFile()) {
                return location;
            }
        }
        throw new Error("fdbserver not found.");
    }


}

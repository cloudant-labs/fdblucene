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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import java.util.Arrays;
import java.util.Collection;

import org.apache.lucene.codecs.lucene80.Lucene80Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;

public class StressFDBDirectoryTest {

    private static Database DB;

    @BeforeClass
    public static void setupFDB() {
        FDB.selectAPIVersion(600);
        DB = FDB.instance().open();
    }

    private final Directory dir;
    private final int pageSize;
    private final int txnSize;

    public StressFDBDirectoryTest() {
        final Path path = FileSystems.getDefault().getPath("lucene", "test");
        this.pageSize = 1_000;
        this.txnSize = 10_000;
        this.dir = FDBDirectory.open(DB, path, pageSize, txnSize);
    }

    @Before
    public void setupDir() throws Exception {
        cleanupDir();
    }

    @After
    public void cleanupDir() throws Exception {
        if (dir == null) {
            return;
        }
        cleanupDir(dir);
    }

    private void cleanupDir(final Directory dir) throws Exception {
        for (final String name : dir.listAll()) {
            dir.deleteFile(name);
        }
    }

    @Test
    public void basicOutput() throws Exception {
        assertEquals(0, dir.listAll().length);
        final IndexOutput out = dir.createOutput("foo", null);
        out.close();
        assertEquals(1, dir.listAll().length);
        dir.deleteFile("foo");
        assertEquals(0, dir.listAll().length);
    }

    @Test
    public void parallelStressCreateOutput() throws Exception {
        // This test doesn't close or delete any of the "files"!
        final int nThreads = 96;
        ExecutorService ex = Executors.newFixedThreadPool(nThreads);
        for (int i = 0; i < 1000; i++) {
            ex.execute(new Worker(i, false));
        }
        ex.shutdown();
        while (!ex.isTerminated()) {
        }
        System.out.println("Finished all threads");
    }

    private class Worker implements Runnable {
        final private int workerId;
        final private Boolean verbose;

        public Worker(int workerId, Boolean verbose) {
            this.workerId = workerId;    
            this.verbose = verbose;
        }

        @Override public void run() {
            if (this.verbose) {
                System.out.println(String.format("Running a worker: %d.", this.workerId)) ;
            }
            try {
                final IndexOutput out = dir.createOutput(String.format("foo_%d", this.workerId), null);
//                out.close();
//                dir.deleteFile(String.format("foo_%d", this.workerId));
            } catch (final IOException e) {
                // nada
            }
        }
    }
}
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

import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;

@RunWith(Parameterized.class)
public class BoundaryTest {

    private static Database DB;
    private static FDBDirectory DIR;

    @Parameters
    public static Collection<Integer> data() {
        return Arrays.asList(
                0,
                1,
                FDBUtil.DEFAULT_PAGE_SIZE - 2,
                FDBUtil.DEFAULT_PAGE_SIZE - 1,
                FDBUtil.DEFAULT_PAGE_SIZE,
                FDBUtil.DEFAULT_PAGE_SIZE + 1,
                FDBUtil.DEFAULT_PAGE_SIZE + 2,

                (2 * FDBUtil.DEFAULT_PAGE_SIZE) - 2,
                (2 * FDBUtil.DEFAULT_PAGE_SIZE) - 1,
                (2 * FDBUtil.DEFAULT_PAGE_SIZE),
                (2 * FDBUtil.DEFAULT_PAGE_SIZE) + 1,
                (2 * FDBUtil.DEFAULT_PAGE_SIZE) + 2);
    }

    @BeforeClass
    public static void setupClass() {
        FDB.selectAPIVersion(600);
        DB = FDB.instance().open();
        final Path path = FileSystems.getDefault().getPath("lucene", "test");
        DIR = FDBDirectory.open(DB, path);
    }

    @AfterClass
    public static void cleanupDir() throws Exception {
        if (DIR == null) {
            return;
        }
        for (final String name : DIR.listAll()) {
            DIR.deleteFile(name);
        }
    }

    private final int size;

    public BoundaryTest(final int size) {
        this.size = size;
    }

    @Test
    public void testSingleWrite() throws Exception {
        final byte[] expected = FDBTestUtil.testArray(size);
        final IndexOutput out = DIR.createTempOutput("foo", "bar", null);
        out.writeBytes(expected, size);
        out.close();

        final byte[] actual = new byte[expected.length];
        final IndexInput in = DIR.openInput(out.getName(), null);
        in.readBytes(actual, 0, size);
        Assert.assertArrayEquals(expected, actual);
    }

    @Test
    public void testRandomWrites() throws Exception {
        final byte[] expected = FDBTestUtil.testArray(size);
        final IndexOutput out = DIR.createTempOutput("foo", "bar", null);

        int remaining = size;
        final Random rnd = new Random();
        while (remaining > 0) {
            final int count = Math.min(remaining, rnd.nextInt(20));
            out.writeBytes(expected, size - remaining, count);
            remaining -= count;
        }

        out.close();

        final byte[] actual = new byte[expected.length];
        final IndexInput in = DIR.openInput(out.getName(), null);
        in.readBytes(actual, 0, size);
        Assert.assertArrayEquals(expected, actual);
    }

    @Test
    public void testRandomReads() throws Exception {
        final byte[] expected = FDBTestUtil.testArray(size);
        final IndexOutput out = DIR.createTempOutput("foo", "bar", null);
        out.writeBytes(expected, size);
        out.close();

        final byte[] actual = new byte[expected.length];
        final IndexInput in = DIR.openInput(out.getName(), null);
        int remaining = size;
        final Random rnd = new Random();
        while (remaining > 0) {
            final int count = Math.min(remaining, rnd.nextInt(20));
            in.readBytes(actual, size - remaining, count);
            remaining -= count;
        }
        Assert.assertArrayEquals(expected, actual);
    }

}

package com.cloudant.fdblucene;

import static org.junit.Assert.*;

import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
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
                FDBUtil.PAGE_SIZE - 2,
                FDBUtil.PAGE_SIZE - 1,
                FDBUtil.PAGE_SIZE,
                FDBUtil.PAGE_SIZE + 1,
                FDBUtil.PAGE_SIZE + 2,

                (2 * FDBUtil.PAGE_SIZE) - 2,
                (2 * FDBUtil.PAGE_SIZE) - 1,
                (2 * FDBUtil.PAGE_SIZE),
                (2 * FDBUtil.PAGE_SIZE) + 1,
                (2 * FDBUtil.PAGE_SIZE) + 2
        );
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
    public void test() throws Exception {
        final byte[] expected = FDBTestUtil.testArray(size);
        final IndexOutput out = DIR.createTempOutput("foo", "bar", null);
        out.writeBytes(expected, size);
        out.close();

        final byte[] actual = new byte[expected.length];
        final IndexInput in = DIR.openInput(out.getName(), null);
        in.readBytes(actual, 0, size);
        Assert.assertArrayEquals(expected, actual);
    }

}

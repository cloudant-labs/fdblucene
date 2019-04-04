package com.cloudant.fdblucene;

import static org.junit.Assert.assertEquals;

import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.util.Arrays;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;

public class SimpleFDBDirectoryTest {

    private static Database DB;

    @BeforeClass
    public static void setupFDB() {
        FDB.selectAPIVersion(600);
        DB = FDB.instance().open();
    }

    private Directory dir;

    @Before
    public void setupDir() throws Exception {
        final Path path = FileSystems.getDefault().getPath("lucene", "test");
        dir = FDBDirectory.open(DB, path);
        cleanupDir();
    }
    
    @After
    public void cleanupDir() throws Exception {
        if (dir == null) {
            return;
        }
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
    public void writeMoreData() throws Exception {
        final byte[] expectedBuf = new byte[1024 * 1024];
        for (int i = 0; i < expectedBuf.length; i++) {
            expectedBuf[i] = (byte) (i % 0x7f);
        }

        final IndexOutput out = dir.createOutput("bar", null);
        out.writeBytes(expectedBuf, expectedBuf.length);
        out.close();

        assertEquals(expectedBuf.length, dir.fileLength("bar"));

        final IndexInput in = dir.openInput("bar", null);
        final byte[] actualBuf = new byte[expectedBuf.length];
        in.readBytes(actualBuf, 0, actualBuf.length);
        in.close();

        Assert.assertArrayEquals(expectedBuf, actualBuf);
    }

    @Test
    public void writeSomeData() throws Exception {
        final IndexOutput out = dir.createOutput("baz", null);
        out.writeLong(12L);
        out.close();
        assertEquals(8, dir.fileLength("baz"));
        final IndexInput in = dir.openInput("baz", null);
        assertEquals(12L, in.readLong());
        in.close();
    }

}

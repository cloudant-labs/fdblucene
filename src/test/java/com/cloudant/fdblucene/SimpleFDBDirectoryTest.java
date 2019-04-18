package com.cloudant.fdblucene;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;

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

@RunWith(Parameterized.class)
public class SimpleFDBDirectoryTest {

    private static Database DB;

    @Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {1_000, 100_000},
                {10_000, 1_000_000},
                {100_000, 1_000_000}});
    }

    @BeforeClass
    public static void setupFDB() {
        FDB.selectAPIVersion(600);
        DB = FDB.instance().open();
    }

    private Directory dir;
    private final int pageSize;
    private final int txnSize;

    public SimpleFDBDirectoryTest(final int pageSize, final int txnSize) {
        this.pageSize = pageSize;
        this.txnSize = txnSize;
    }

    @Before
    public void setupDir() throws Exception {
        final Path path = FileSystems.getDefault().getPath("lucene", "test");
        dir = FDBDirectory.open(DB, path, pageSize, txnSize);
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
        final byte[] expectedBuf = FDBTestUtil.testArray(1024 * 1024);

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

    @Test
    public void indexSomething() throws Exception {
        try (final IndexWriter writer = new IndexWriter(dir, indexWriterConfig())) {
            addDocument(writer, "doc1");
            writer.commit();
        }

        try (final IndexReader reader = DirectoryReader.open(dir)) {
            final IndexSearcher searcher = new IndexSearcher(reader);
            final Query query = new TermQuery(new Term("_id", "doc1"));
            final TopDocs topDocs = searcher.search(query, 1);
            assertEquals(1, topDocs.totalHits.value);
            final Document doc = reader.document(topDocs.scoreDocs[0].doc);
            assertEquals("doc1", doc.get("_id"));
        }
    }

    @Test
    public void addIndexes() throws Exception {
        Directory dir1 = FDBDirectory.open(DB, FileSystems.getDefault().getPath("lucene", "test1"));
        IndexWriter writer1 = new IndexWriter(dir1, indexWriterConfig());
        addDocument(writer1, "foo1");
        writer1.commit();
        writer1.close();

        Directory dir2 = FDBDirectory.open(DB, FileSystems.getDefault().getPath("lucene", "test2"));
        IndexWriter writer2 = new IndexWriter(dir2, indexWriterConfig());
        addDocument(writer2, "foo2");
        writer2.commit();
        writer2.close();

        Directory dir3 = FDBDirectory.open(DB, FileSystems.getDefault().getPath("lucene", "test3"));
        IndexWriter writer3 = new IndexWriter(dir3, indexWriterConfig());
        writer3.addIndexes(dir1, dir2);
        writer3.commit();
        writer3.close();

        try (final IndexReader reader = DirectoryReader.open(dir3)) {
            assertEquals(2, reader.numDocs());
        }
    }

    private void addDocument(final IndexWriter writer, final String docId) throws IOException {
        final Document doc = new Document();
        doc.add(new TextField("foo", "hello everybody", Store.NO));
        doc.add(new StringField("_id", docId, Store.YES));
        writer.addDocument(doc);
    }

    private IndexWriterConfig indexWriterConfig() {
        final IndexWriterConfig config = new IndexWriterConfig();
        config.setUseCompoundFile(false);
        config.setCodec(new Lucene80Codec());
        return config;
    }

}

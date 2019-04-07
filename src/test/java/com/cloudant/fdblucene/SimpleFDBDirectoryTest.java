package com.cloudant.fdblucene;

import static org.junit.Assert.assertEquals;

import java.nio.file.FileSystems;
import java.nio.file.Path;

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
        final IndexWriterConfig config = new IndexWriterConfig();
        config.setUseCompoundFile(false);
        config.setCodec(new Lucene80Codec());

        try (final IndexWriter writer = new IndexWriter(dir, config)) {
            final Document doc = new Document();
            doc.add(new TextField("foo", "hello everybody", Store.NO));
            doc.add(new StringField("_id", "doc1", Store.YES));
            writer.addDocument(doc);
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

}

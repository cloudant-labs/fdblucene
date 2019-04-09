package com.cloudant.fdblucene;

import java.nio.file.FileSystem;
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
import org.apache.lucene.store.NIOFSDirectory;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;

@RunWith(Parameterized.class)
public class PerformanceComparisonTest {

    @Parameters
    public static Collection<Directory> data() throws Exception {
        FDB.selectAPIVersion(600);
        Database db = FDB.instance().open();

        FileSystem fileSystem = FileSystems.getDefault();
        final Path javaTempDir = fileSystem
                .getPath(System.getProperty("tempDir", System.getProperty("java.io.tmpdir")));
        final Path tmpDir = javaTempDir.resolve("foo");

        return Arrays.asList(FDBDirectory.open(db, tmpDir), new NIOFSDirectory
                (tmpDir));
    }

    private Directory dir;

    public PerformanceComparisonTest(Directory dir) {
        this.dir = dir;
    }

    @After
    public void cleanupDir() throws Exception {
        for (final String name : dir.listAll()) {
            dir.deleteFile(name);
        }
    }

    @Test
    public void measureIndexingSpeed() throws Exception {
        final IndexWriterConfig config = new IndexWriterConfig();
        config.setUseCompoundFile(false);
        config.setCodec(new Lucene80Codec());

        try (final IndexWriter writer = new IndexWriter(dir, config)) {
            final int docCount = 500_000;
            final long start = System.currentTimeMillis();
            for (int i = 0; i < docCount; i++) {
                final Document doc = new Document();
                doc.add(new TextField("foo", "Duis aute irure dolor in reprehenderit in voluptate", Store.NO));
                doc.add(new StringField("_id", String.format("doc%d", i), Store.YES));
                writer.addDocument(doc);
            }
            writer.commit();
            writer.close();
            final long duration = System.currentTimeMillis() - start;
            final long docsPerSecond = (docCount * 1000) / duration;
            System.out.printf(dir.getClass().getName() + " indexed at %,d docs per second.\n", docsPerSecond);
        }
    }

    @Test
    public void measuringSearchSpeed() throws Exception {
        measureIndexingSpeed();

        try (final IndexReader reader = DirectoryReader.open(dir)) {
            final IndexSearcher searcher = new IndexSearcher(reader);
            final int searchCount = 20_000;
            final long start = System.currentTimeMillis();
            for (int i = 0; i < searchCount; i++) {
                final Query query = new TermQuery(new Term("_id", String.format("doc%d", i)));
                final TopDocs topDocs = searcher.search(query, 1);
            }
            final long duration = System.currentTimeMillis() - start;
            final long searchesPerSecond = (searchCount * 1000) / duration;
            System.out.printf(dir.getClass().getName() + " searches at %,d docs per second.\n", searchesPerSecond);
        }
    }

}

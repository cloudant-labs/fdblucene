package com.cloudant.fdblucene;

import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Random;

import org.apache.lucene.codecs.lucene80.Lucene80Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.LineFileDocs;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.Test;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class FDBLuceneIndexingPerformanceTest {

    private final Random random = new Random();

    @Test
    public void test() throws Exception {
        FDB.selectAPIVersion(600);
        Database db = FDB.instance().open();

        FileSystem fileSystem = FileSystems.getDefault();
        final Path javaTempDir = fileSystem
                .getPath(System.getProperty("tempDir", System.getProperty("java.io.tmpdir")));
        final Path tmpDir = javaTempDir.resolve("foo");

        final Directory fdbDir = FDBDirectory.open(db, tmpDir);
        final Directory fsDir = new NIOFSDirectory(tmpDir);

        runIndexing(fdbDir);
        runSearches(fdbDir);
        runIndexing(fsDir);
        runSearches(fsDir);
        fdbDir.close();
        fsDir.close();
    }

    private void runIndexing(final Directory dir)
            throws IOException {
        cleanup(dir);
        final IndexWriterConfig config = new IndexWriterConfig();
        config.setUseCompoundFile(false);
        config.setCodec(new Lucene80Codec());

        try (final LineFileDocs docs = new LineFileDocs(random, LuceneTestCase.DEFAULT_LINE_DOCS_FILE);
                final IndexWriter writer = new IndexWriter(dir, config)) {

            final long maxDocCount = 100000;
            final long start = System.currentTimeMillis();
            long docCount = 0;
            Document doc = docs.nextDoc();
            final StringField idField = new StringField("_id", "", Store.YES);
            doc.add(idField);

            for (int i = 0; i < maxDocCount; i++) {
                doc = docs.nextDoc();
                idField.setStringValue("doc-" + i);
                docCount++;
                writer.addDocument(doc);
            }
            writer.commit();

            final long duration = System.currentTimeMillis() - start;
            final long docsPerSecond = (docCount * 1000) / duration;
            System.out.printf(dir.getClass().getName() + " indexed at %d docs per second.\n", docsPerSecond);
        }
    }

    private void runSearches(final Directory dir) throws IOException {
        final IndexReader reader = DirectoryReader.open(dir);
        final IndexSearcher searcher = new IndexSearcher(reader);
        final long maxQueryCount = 50000;
        long queryCount = 0;

        final long start = System.currentTimeMillis();
        for (int i = 0; i < maxQueryCount; i++) {
            searcher.search(new TermQuery(new Term("_id", "doc-" + random.nextInt(1000))),  10);
            queryCount++;
        }
        final long duration = System.currentTimeMillis() - start;
        final long queriesPerSecond = (queryCount * 1000) / duration;
        System.out.printf(dir.getClass().getName() + " searches at %d docs per second.\n", queriesPerSecond);
    }

    private void cleanup(final Directory dir) throws IOException {
        for (final String name : dir.listAll()) {
            dir.deleteFile(name);
        }
    }

}

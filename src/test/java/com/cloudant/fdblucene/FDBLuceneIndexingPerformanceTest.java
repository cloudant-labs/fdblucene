package com.cloudant.fdblucene;

import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;

import org.apache.lucene.codecs.lucene80.Lucene80Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.LineFileDocs;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.Test;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class FDBLuceneIndexingPerformanceTest extends LuceneTestCase {

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
        runIndexing(fsDir);
        fdbDir.close();
        fsDir.close();
    }

    private void runIndexing(final Directory dir)
            throws IOException {
        final IndexWriterConfig config = new IndexWriterConfig();
        config.setUseCompoundFile(false);
        config.setMergePolicy(NoMergePolicy.INSTANCE);
        config.setCodec(new Lucene80Codec());

        try (final LineFileDocs docs = new LineFileDocs(random(), LuceneTestCase.DEFAULT_LINE_DOCS_FILE);
                final IndexWriter writer = new IndexWriter(dir, config)) {

            final long maxDocCount = 100000;
            final long start = System.currentTimeMillis();
            long docCount = 0;
            for (int i = 0; i < maxDocCount; i++) {
                final Document doc = docs.nextDoc();
                if (doc == null) {
                    break;
                }
                docCount++;
                writer.addDocument(doc);
            }
            writer.commit();

            final long duration = System.currentTimeMillis() - start;
            final long docsPerSecond = (docCount * 1000) / duration;
            System.out.printf(dir.getClass().getName() + " indexed at %d docs per second.\n", docsPerSecond);
        }
    }

}

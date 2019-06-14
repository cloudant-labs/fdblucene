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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.subspace.Subspace;

/**
 * Tests that demonstrate equivalence with normal Lucene code.
 */
public class ScoreTest {

    private static final Analyzer ANALYZER = new StandardAnalyzer();

    private static final boolean VERBOSE = false;

    private static Database DB;
    private Subspace subspace1;
    private Subspace subspace2;

    @BeforeClass
    public static void setupFDB() {
        FDB.selectAPIVersion(600);
        DB = FDB.instance().open();
    }

    @Before
    public void setup() throws Exception {
        subspace1 = new Subspace(new byte[] { 1 });
        subspace2 = new Subspace(new byte[] { 2 });
        cleanup();
    }

    @After
    public void cleanup() throws Exception {
        DB.run(txn -> {
            txn.clear(subspace1.range());
            txn.clear(subspace2.range());
            return null;
        });
    }

    @Test
    public void noRepeatedTerm() throws IOException {
        final Document doc1 = doc("text", "hello there you");
        compareScores(new TermQuery(new Term("text", "hello")), doc1);
    }

    @Test
    public void repeatedTerm() throws IOException {
        final Document doc1 = doc("text", "hello there you hello");
        compareScores(new TermQuery(new Term("text", "hello")), doc1);
    }

    @Test
    public void prefix() throws IOException {
        final Document doc1 = doc("text", "hello there you hello");
        final Document doc2 = doc("text", "hello there you hello");
        final Document doc3 = doc("text", " there you ");
        compareScores(new PrefixQuery(new Term("text", "hell")), doc1, doc2, doc3);
    }

    @Test
    public void phrase() throws IOException {
        final Document doc1 = doc("text", "hello there you hello");
        final Document doc2 = doc("text", "hello there you hello");
        final Document doc3 = doc("text", " there you ");
        compareScores(new PhraseQuery("text", "you", "hello"), doc1, doc2, doc3);
    }

    private void compareScores(final Query query, final Document... docs) throws IOException {
        final TopDocs td1 = search("FDBDirectory", query, indexWithFDBDirectory(docs));
        final TopDocs td2 = search("FDBIndexWriter", query, indexWithFDBIndexWriter(docs));
        assertEquals(td1.totalHits, td2.totalHits);
        assertEquals(td1.scoreDocs.length, td2.scoreDocs.length);
        for (int i = 0; i < td1.scoreDocs.length; i++) {
            assertEquals(td1.scoreDocs[i].score, td2.scoreDocs[i].score, 0.0);
        }
    }

    private IndexReader indexWithFDBDirectory(final Document... docs) throws IOException {
        final FDBDirectory dir = FDBDirectory.open(DB, subspace1, FDBUtil.DEFAULT_PAGE_SIZE, FDBUtil.DEFAULT_TXN_SIZE);
        final IndexWriterConfig config = new IndexWriterConfig(ANALYZER);
        try (final IndexWriter writer = new IndexWriter(dir, config)) {
            for (final Document doc : docs) {
                writer.addDocument(doc);
            }
            writer.commit();
        }
        return DirectoryReader.open(dir);
    }

    private IndexReader indexWithFDBIndexWriter(final Document... docs) throws IOException {
        final FDBIndexWriter writer = new FDBIndexWriter(DB, subspace2, ANALYZER);
        for (final Document doc : docs) {
            writer.addDocument(doc);
        }
        return new FDBIndexReader(DB, subspace2);
    }

    private TopDocs search(final String prefix, final Query query, final IndexReader reader) throws IOException {
        final IndexSearcher searcher = new IndexSearcher(reader);
        final TopDocs result = searcher.search(query, 1);

        if (VERBOSE) {
            for (final ScoreDoc doc : result.scoreDocs) {
                System.out.printf("%s\n%s\n", prefix, searcher.explain(query, doc.doc));
            }
        }

        return result;
    }

    private Document doc(final String field, String text) {
        final Document result = new Document();
        result.add(new TextField(field, text, Store.NO));
        return result;
    }

}

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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Collections;
import java.util.Random;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleDocValuesField;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortField.Type;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.util.BytesRef;
import org.junit.Before;
import org.junit.Test;

import com.apple.foundationdb.Transaction;

public class FDBIndexReaderWriterTest extends BaseFDBTest {

    private Random random = new Random();

    private FDBIndexReader reader;
    private IndexSearcher searcher;

    private int docID1;
    private int docID2;
    private int docID3;

    @Before
    public void setupIndex() throws Exception {
        final Analyzer analyzer = new StandardAnalyzer();

        final FDBIndexWriter writer = new FDBIndexWriter(DB, subspace, analyzer);

        docID1 = writer.addDocument(doc("hello", "abc def ghi"));
        final int[] ids = writer.addDocuments(doc("bye", "abc ghi def"), doc("hell", "abc def ghi def def"));
        docID2 = ids[0];
        docID3 = ids[1];

        reader = new FDBIndexReader(subspace);
        this.searcher = new IndexSearcher(reader);
    }

    @Test
    public void allStoredFields() throws Exception {
        reader.run(DB, () -> { // txn around search and doc fetch
            final TopDocs topDocs = searcher.search(new TermQuery(new Term("body", "def")), 5);
            assertEquals(3, topDocs.totalHits.value);

            final Document hit = reader.document(topDocs.scoreDocs[0].doc);
            assertEquals(123.456f, hit.getField("float").numericValue());
            assertEquals(123.456, hit.getField("double").numericValue());
            return null;
        });
    }

    @Test
    public void someStoredFields() throws Exception {
        reader.run(DB, () -> { // txn around search and doc fetch
            final TopDocs topDocs = searcher.search(new TermQuery(new Term("body", "def")), 5);
            assertEquals(3, topDocs.totalHits.value);

            final Document hit = reader.document(topDocs.scoreDocs[0].doc, Collections.singleton("float"));
            assertEquals(123.456f, hit.getField("float").numericValue());
            assertNull(hit.getField("double"));
            return null;
        });
    }

    @Test
    public void termQuery() throws Exception {
        final TopDocs topDocs = search(new TermQuery(new Term("body", "def")), 5);
        assertEquals(3, topDocs.totalHits.value);
    }

    @Test
    public void prefixQuery() throws Exception {
        final TopDocs topDocs = search(new PrefixQuery(new Term("_id", "hel")), 2);
        assertEquals(2, topDocs.totalHits.value);
    }

    @Test
    public void prefixQuery2() throws Exception {
        final TopDocs topDocs = search(new PrefixQuery(new Term("_id", "hell")), 2);
        assertEquals(2, topDocs.totalHits.value);
    }

    @Test
    public void phraseQuery() throws Exception {
        final TopDocs topDocs = search(new PhraseQuery("body", "abc", "def"), 1);
        assertEquals(2, topDocs.totalHits.value);
    }

    @Test
    public void phraseQuery2() throws Exception {
        final TopDocs topDocs = search(new PhraseQuery("body", "def", "def"), 1);
        assertEquals(1, topDocs.totalHits.value);
    }

    @Test
    public void numericSorting() throws Exception {
        final Query query = new TermQuery(new Term("body", "def"));
        final Sort sort = new Sort(new SortField("double-sort", Type.DOUBLE));
        final TopFieldDocs topDocs = search(query, 10, sort);
        assertEquals(3, topDocs.totalHits.value);
        double low = 0.0;
        for (int i = 0; i < topDocs.totalHits.value; i++) {
            final Double order = (Double) ((FieldDoc) topDocs.scoreDocs[i]).fields[0];
            assertTrue(order >= low);
            low = order;
        }
    }

    @Test
    public void stringSorting() throws Exception {
        final Query query = new TermQuery(new Term("body", "def"));
        final Sort sort = new Sort(new SortField("str-sort", Type.STRING_VAL));
        final TopFieldDocs topDocs = search(query, 10, sort);
        assertEquals(3, topDocs.totalHits.value);
        BytesRef low = new BytesRef(BytesRef.EMPTY_BYTES);
        for (int i = 0; i < topDocs.totalHits.value; i++) {
            final BytesRef order = (BytesRef) ((FieldDoc) topDocs.scoreDocs[i]).fields[0];
            assertTrue(order.compareTo(low) >= 0);
            low = order;
        }
    }

    @Test
    public void testFDBPostingsEnum() throws Exception {
        final Transaction txn = DB.createTransaction();
        try {
            final PostingsEnum p = new FDBPostingsEnum(txn, subspace, "body", new BytesRef("def"));

            for (int i = 0; i < 3; i++) {
                int nextID = p.nextDoc();
                if (nextID == docID1) {
                    assertEquals(1, p.nextPosition());
                }
                if (nextID == docID2) {
                    assertEquals(2, p.nextPosition());
                }
                if (nextID == docID3) {
                    assertEquals(1, p.nextPosition());
                    assertEquals(3, p.nextPosition());
                    assertEquals(4, p.nextPosition());
                }
            }
            assertEquals(DocIdSetIterator.NO_MORE_DOCS, p.nextDoc());
        } finally {
            txn.commit().join();
            txn.close();
        }
    }

    @Test
    public void booleanQuery() throws Exception {
        final BooleanQuery.Builder builder = new BooleanQuery.Builder();
        builder.add(new TermQuery(new Term("body", "def")), Occur.MUST);
        builder.add(new PrefixQuery(new Term("_id", "hell")), Occur.MUST);

        final TopDocs topDocs = search(builder.build(), 1);
        assertEquals(2, topDocs.totalHits.value);
    }

    @Test
    public void doubleExactQuery() throws Exception {
        Query query = FDBNumericPoint.newExactQuery("double-pt", 5.5);
        final TopDocs topDocs = search(query, 5);
        assertEquals(3, topDocs.totalHits.value);
    }

    @Test
    public void doubleRangeQuery() throws Exception {
        Query query = FDBNumericPoint.newRangeQuery("double-pt", 4.0, 6.0);
        final TopDocs topDocs = search(query, 5);
        assertEquals(3, topDocs.totalHits.value);
    }

    @Test
    public void longExactQuery() throws Exception {
        Query query = FDBNumericPoint.newExactQuery("long-pt", 17L);
        final TopDocs topDocs = search(query, 5);
        assertEquals(3, topDocs.totalHits.value);
    }

    @Test
    public void longRangeQuery() throws Exception {
        Query query = FDBNumericPoint.newRangeQuery("long-pt", 16L, 18L);
        final TopDocs topDocs = search(query, 5);
        assertEquals(3, topDocs.totalHits.value);
    }

    private Document doc(final String id, final String body) {
        final Document result = new Document();
        result.add(new StringField("_id", id, Store.YES));
        result.add(new TextField("body", body, Store.NO));

        // For sorting
        result.add(new DoubleDocValuesField("double-sort", random.nextDouble()));
        result.add(new NumericDocValuesField("long-sort", random.nextLong()));
        result.add(new BinaryDocValuesField("str-sort", new BytesRef(RandomStringUtils.randomAlphabetic(3,  10))));

        // For querying
        result.add(new FDBNumericPoint("double-pt", 5.5));
        result.add(new FDBNumericPoint("long-pt", 17L));

        // For retrieval
        result.add(new StoredField("float", 123.456f));
        result.add(new StoredField("double", 123.456));
        return result;
    }

    private TopDocs search(final Query query, final int count) throws IOException {
        return reader.run(DB, () -> {
            return searcher.search(query, count);
        });
    }

    private TopFieldDocs search(final Query query, final int count, final Sort sort) throws IOException {
        return reader.run(DB, () -> {
            return searcher.search(query, count, sort);
        });
    }

}

package com.cloudant.fdblucene;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Random;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleDocValuesField;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexReader;
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

public class FDBIndexReaderWriterTest extends BaseFDBTest {

    private Random random = new Random();

    private IndexReader reader;
    private IndexSearcher searcher;

    private int docID1;
    private int docID2;
    private int docID3;

    @Before
    public void setupIndex() throws Exception {
        final Analyzer analyzer = new StandardAnalyzer();

        final FDBIndexWriter writer = new FDBIndexWriter(DB, subspace, analyzer);
        docID1 = writer.addDocument(doc("hello", "abc def ghi"));
        docID2 = writer.addDocument(doc("bye", "abc ghi def"));
        docID3 = writer.addDocument(doc("hell", "abc def ghi def def"));

        reader = new FDBIndexReader(DB, subspace);
        this.searcher = new IndexSearcher(reader);
    }

    @Test
    public void storedFields() throws Exception {
        final TopDocs topDocs = searcher.search(new TermQuery(new Term("body", "def")), 5);
        assertEquals(3, topDocs.totalHits.value);

        final Document hit = reader.document(topDocs.scoreDocs[0].doc);
        assertEquals(123.456f, hit.getField("float").numericValue());
        assertEquals(123.456, hit.getField("double").numericValue());

    }

    @Test
    public void termQuery() throws Exception {
        final TopDocs topDocs = searcher.search(new TermQuery(new Term("body", "def")), 5);
        assertEquals(3, topDocs.totalHits.value);
    }

    @Test
    public void prefixQuery() throws Exception {
        final TopDocs topDocs = searcher.search(new PrefixQuery(new Term("_id", "hell")), 1);
        assertEquals(2, topDocs.totalHits.value);
    }

    @Test
    public void phraseQuery() throws Exception {
        final TopDocs topDocs = searcher.search(new PhraseQuery("body", "abc", "def"), 1);
        assertEquals(2, topDocs.totalHits.value);
    }

    @Test
    public void phraseQuery2() throws Exception {
        final TopDocs topDocs = searcher.search(new PhraseQuery("body", "def", "def"), 1);
        assertEquals(1, topDocs.totalHits.value);
    }

    @Test
    public void sorting() throws Exception {
        final Query query = new TermQuery(new Term("body", "def"));
        final Sort sort = new Sort(new SortField("double-sort", Type.DOUBLE));
        final TopFieldDocs topDocs = searcher.search(query, 10, sort);
        assertEquals(3, topDocs.totalHits.value);
        double low = 0.0;
        for (int i = 0; i < topDocs.totalHits.value; i++) {
            final Double order = (Double) ((FieldDoc) topDocs.scoreDocs[i]).fields[0];
            assertTrue(order >= low);
            low = order;
        }
    }

    @Test
    public void testFDBPostingsEnum() throws Exception {
        final PostingsEnum p = new FDBPostingsEnum(DB, subspace, "body", new BytesRef("def"));

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
    }

    @Test
    public void booleanQuery() throws Exception {
        final BooleanQuery.Builder builder = new BooleanQuery.Builder();
        builder.add(new TermQuery(new Term("body", "def")), Occur.MUST);
        builder.add(new PrefixQuery(new Term("_id", "hell")), Occur.MUST);

        final TopDocs topDocs = searcher.search(builder.build(), 1);
        assertEquals(2, topDocs.totalHits.value);
    }

    @Test
    public void doubleExactQuery() throws Exception {
        Query query = FDBNumericPoint.newExactQuery("double-pt", 5.5);
        final TopDocs topDocs = searcher.search(query, 5);
        assertEquals(3, topDocs.totalHits.value);
    }

    @Test
    public void doubleRangeQuery() throws Exception {
        Query query = FDBNumericPoint.newRangeQuery("double-pt", 4.0, 6.0);
        final TopDocs topDocs = searcher.search(query, 5);
        assertEquals(3, topDocs.totalHits.value);
    }

    @Test
    public void longExactQuery() throws Exception {
        Query query = FDBNumericPoint.newExactQuery("long-pt", 17L);
        final TopDocs topDocs = searcher.search(query, 5);
        assertEquals(3, topDocs.totalHits.value);
    }

    @Test
    public void longRangeQuery() throws Exception {
        Query query = FDBNumericPoint.newRangeQuery("long-pt", 16L, 18L);
        final TopDocs topDocs = searcher.search(query, 5);
        assertEquals(3, topDocs.totalHits.value);
    }

    private Document doc(final String id, final String body) {
        final Document result = new Document();
        result.add(new StringField("_id", id, Store.YES));
        result.add(new TextField("body", body, Store.NO));

        // For sorting
        result.add(new DoubleDocValuesField("double-sort", random.nextDouble()));
        result.add(new NumericDocValuesField("long-sort", random.nextLong()));

        // For querying
        result.add(new FDBNumericPoint("double-pt", 5.5));
        result.add(new FDBNumericPoint("long-pt", 17L));

        // For retrieval
        result.add(new StoredField("float", 123.456f));
        result.add(new StoredField("double", 123.456));
        return result;
    }

}

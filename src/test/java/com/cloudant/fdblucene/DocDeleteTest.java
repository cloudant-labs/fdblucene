package com.cloudant.fdblucene;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleDocValuesField;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.Term;
import org.junit.Test;

import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;

public class DocDeleteTest extends BaseFDBTest {

    @Test
    public void delete() throws Exception {
        final Analyzer analyzer = new StandardAnalyzer();
        final FDBIndexWriter writer = new FDBIndexWriter(DB, subspace, analyzer);

        writer.addDocument(doc("foo", "bar baz, foobar."));
        assertTrue("nothing got indexed", debrisReport(subspace) > 0);
        writer.deleteDocuments(new Term("_id", "foo"));
        assertEquals("debris left in index.", 0, debrisReport(subspace));
    }

    @Test
    public void update() throws Exception {
        final Analyzer analyzer = new StandardAnalyzer();
        final FDBIndexWriter writer = new FDBIndexWriter(DB, subspace, analyzer);

        writer.addDocument(doc("foo", "bar baz, foobar."));
        int debrisCount = debrisReport(subspace);
        assertTrue("nothing got indexed", debrisCount > 0);

        writer.updateDocument(new Term("_id", "foo"), doc("foo", "bar baz, foobar."));
        assertEquals("debris left in index.", debrisCount, debrisReport(subspace));
    }

    @Test
    public void multipleDocs() throws Exception {
        final Analyzer analyzer = new StandardAnalyzer();
        final FDBIndexWriter writer = new FDBIndexWriter(DB, subspace, analyzer);

        for (int i = 0; i < 100; i++) {
            writer.addDocument(doc("doc-" + i, "bar baz, foobar."));
        }
        assertTrue("nothing got indexed", debrisReport(subspace) > 0);

        for (int i = 0; i < 100; i++) {
            writer.deleteDocuments(new Term("_id", "doc-" + i));
            writer.deleteDocuments(new Term("_id", "foo"));
        }

        assertEquals("debris left in index.", 0, debrisReport(subspace));
    }

    private Document doc(final String id, final String body) {
        final Document result = new Document();
        result.add(new StringField("_id", id, Store.YES));
        result.add(new TextField("stored-body", body, Store.YES));
        result.add(new TextField("unstored-body", body, Store.NO));
        result.add(new DoubleDocValuesField("double-sort", 12.5));
        result.add(new NumericDocValuesField("long-sort", 15));
        result.add(new FDBNumericPoint("double-pt", 5.5));
        result.add(new StoredField("float", 123.456f));

        return result;
    }

    private int debrisReport(final Subspace s) {
        final AtomicInteger counter = new AtomicInteger();
        DB.run(txn -> {
            txn.getRange(s.range()).forEach(kv -> {
                final Tuple key = subspace.unpack(kv.getKey());
                final String value = Arrays.toString(kv.getValue());
                System.out.printf("%s %s\n", key, value);
                counter.getAndIncrement();
            });
            return null;
        });
        return counter.get();
    }

}

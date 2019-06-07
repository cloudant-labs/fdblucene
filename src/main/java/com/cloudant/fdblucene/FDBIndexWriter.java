package com.cloudant.fdblucene;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TermFrequencyAttribute;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;

public final class FDBIndexWriter {

    private final Random random = new Random();

    private static final byte[] EMPTY = new byte[0];
    private static final byte[] ONE = new byte[] { 1, 0, 0, 0, 0, 0, 0, 0 };

    private final Database db;
    private final Subspace index;
    private final Analyzer analyzer;

    public FDBIndexWriter(final Database db, final Subspace index, final Analyzer analyzer) {
        this.db = db;
        this.index = index;
        this.analyzer = analyzer;
    }

    public int addDocument(final Document doc) throws IOException {
        while (true) {
            final int docID = randomDocID();
            try {
                db.run(txn -> {
                    Utils.trace(txn, "addDocument(%d)", docID);
                    addDocument(txn, docID, doc);
                    txn.mutate(MutationType.ADD, FDBAccess.numDocsKey(index), ONE);
                    return null;
                });
            } catch (final CompletionException e) {
                if (e.getCause() instanceof DocIdCollisionException) {
                    // Try again.
                    continue;
                }
                throw e;
            }
            return docID;
        }

    }

    public void addDocuments(final Document... docs) throws IOException {
        while (true) {
            final int[] ids = generateUniqueIDs(docs);
            try {
                db.run(txn -> {
                    Utils.trace(txn, "addDocuments(%d)", docs.length);
                    for (int i = 0; i < docs.length; i++) {
                        addDocument(txn, ids[i], docs[i]);
                    }
                    txn.mutate(MutationType.ADD, FDBAccess.numDocsKey(index), ByteArrayUtil.encodeInt(docs.length));
                    return null;
                });
            } catch (final CompletionException e) {
                if (e.getCause() instanceof DocIdCollisionException) {
                    // Try again.
                    continue;
                }
                throw e;
            }
            return;
        }
    }

    private int[] generateUniqueIDs(final Document... docs) {
        final int[] result = new int[docs.length];
        final Set<Integer> seen = new HashSet<Integer>();
        do {
            seen.clear();
            for (int i = 0; i < docs.length; i++) {
                result[i] = randomDocID();
                seen.add(result[i]);
            }
        } while (seen.size() != docs.length);
        return result;
    }

    private void addDocument(final Transaction txn, final int docID, final Document doc)
            throws DocIdCollisionException {
        final CompletableFuture<byte[]> future = docIDFuture(txn, docID);
        txn.set(FDBAccess.docIDKey(index, docID), EMPTY);
        for (final IndexableField field : doc) {
            try {
                indexField(txn, docID, field);
            } catch (final IOException e) {
                throw new CompletionException(e);
            }
        }
        if (future.join() != null) {
            throw new DocIdCollisionException(docID);
        }
    }

    private CompletableFuture<byte[]> docIDFuture(final Transaction txn, final int docID) {
        return txn.get(FDBAccess.docIDKey(index, docID));
    }

    public void deleteDocument(final Query... queries) throws IOException {
        throw new UnsupportedOperationException("deleteDocument not supported.");
    }

    public void deleteDocument(final Term... terms) {
        throw new UnsupportedOperationException("deleteDocument not supported.");
    }

    public void updateDocument(final Term term, final Iterable<? extends IndexableField> doc) throws IOException {
        throw new UnsupportedOperationException("updateDocument not supported.");
    }

    private void indexField(final Transaction txn, final int docID, final IndexableField field) throws IOException {
        final String fieldName = field.name();
        final IndexableFieldType fieldType = field.fieldType();

        if (fieldType.indexOptions() != IndexOptions.NONE) {
            indexInvertedField(txn, docID, fieldName, field);
        }

        if (fieldType.stored()) {
            indexStoredField(txn, docID, fieldName, field);
        }

        final DocValuesType dvType = fieldType.docValuesType();
        if (dvType != DocValuesType.NONE) {
            indexDocValue(txn, dvType, docID, fieldName, field);
        }

        if (fieldType.pointDataDimensionCount() > 0) {
            indexPoint(txn, docID, fieldName, field);
        }

        txn.mutate(MutationType.ADD, FDBAccess.sumDocFreqKey(index, fieldName), ONE);
    }

    private void indexInvertedField(
            final Transaction txn,
            final int docID,
            final String fieldName,
            final IndexableField field) throws IOException {

        try (final TokenStream stream = field.tokenStream(analyzer, null)) {
            final TermToBytesRefAttribute termAttribute = stream.getAttribute(TermToBytesRefAttribute.class);
            final TermFrequencyAttribute termFreqAttribute = stream.addAttribute(TermFrequencyAttribute.class);
            final PositionIncrementAttribute posIncrAttribute = stream.addAttribute(PositionIncrementAttribute.class);
            final OffsetAttribute offsetAttribute = stream.addAttribute(OffsetAttribute.class);
            final PayloadAttribute payloadAttribute = stream.addAttribute(PayloadAttribute.class);

            int pos = 0;
            stream.reset();
            final Map<BytesRef, Integer> termFreqs = new HashMap<BytesRef, Integer>();
            int length = 0;

            Set<BytesRef> seen = new HashSet<BytesRef>();

            while (stream.incrementToken()) {
                final int posIncr = posIncrAttribute.getPositionIncrement();
                final int startOffset = offsetAttribute.startOffset();
                final int endOffset = offsetAttribute.endOffset();
                final int termFreq = termFreqAttribute.getTermFrequency();
                final BytesRef term = termAttribute.getBytesRef();
                final BytesRef payload = payloadAttribute.getPayload();

                termFreqs.compute(BytesRef.deepCopyOf(term), (k, v) -> {
                    return (v == null) ? 1 : v + 1;
                });

                final byte[] termValue = new byte[16];
                if (!seen.contains(term)) {
                    Utils.encodeInt(termValue, 0, 1);
                    seen.add(term);
                }
                Utils.encodeInt(termValue, 8, termFreq);
                txn.mutate(MutationType.ADD, FDBAccess.termKey(index, fieldName, term), termValue);

                final byte[] postingsKey = FDBAccess.postingsKey(index, fieldName, term, docID, pos);
                final byte[] postingsValue = FDBAccess.postingsValue(startOffset, endOffset, payload);
                txn.set(postingsKey, postingsValue);
                pos += posIncr;
                length++;
            }
            stream.end();

            if (!termFreqs.isEmpty()) {
                termFreqs.forEach((k, v) -> {
                    txn.mutate(
                            MutationType.ADD,
                            FDBAccess.postingsKey(index, fieldName, k, docID),
                            ByteArrayUtil.encodeInt(v));
                });

                txn.mutate(
                        MutationType.ADD,
                        FDBAccess.normKey(index, fieldName, docID),
                        ByteArrayUtil.encodeInt(length));
                txn.mutate(MutationType.ADD, FDBAccess.docCountKey(index, fieldName), ONE);
                txn.mutate(
                        MutationType.ADD,
                        FDBAccess.sumTotalTermFreqKey(index, fieldName),
                        ByteArrayUtil.encodeInt(length));
            }
        }
    }

    private void indexStoredField(
            final Transaction txn,
            final int docID,
            final String fieldName,
            final IndexableField field) {
        final byte[] key = FDBAccess.storedKey(index, docID, fieldName);
        final byte[] value = FDBAccess.storedValue(field);
        txn.set(key, value);
    }

    private void indexDocValue(
            final Transaction txn,
            final DocValuesType dvType,
            final int docID,
            final String fieldName,
            final IndexableField field) {
        switch (dvType) {
        case NUMERIC:
            txn.set(
                    FDBAccess.numericDocValuesKey(index, fieldName, docID),
                    ByteArrayUtil.encodeInt(field.numericValue().longValue()));
            break;
        default:
            throw new IllegalArgumentException("non-numeric DocValue not supported");
        }
    }

    private void indexPoint(
            final Transaction txn,
            final int docID,
            final String fieldName,
            final IndexableField field) {
        throw new IllegalArgumentException("Points not supported");
    }

    private int randomDocID() {
        return random.nextInt(DocIdSetIterator.NO_MORE_DOCS);
    }

}

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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
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
import org.apache.lucene.util.BytesRef;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;

public final class FDBIndexWriter {

    private static final int RETRY_LIMIT = 100;

    private final Random random = new Random();

    private static final byte[] EMPTY = new byte[0];
    private static final byte[] ONE = ByteArrayUtil.encodeInt(1);
    private static final byte[] ZERO = ByteArrayUtil.encodeInt(0);
    private static final byte[] NEGATIVE_ONE = ByteArrayUtil.encodeInt(-1);

    private final Database db;
    private final Subspace index;
    private final Analyzer analyzer;

    public FDBIndexWriter(final Database db, final Subspace index, final Analyzer analyzer) {
        this.db = db;
        this.index = index;
        this.analyzer = analyzer;
    }

    public int addDocument(final Document doc) throws IOException {
        for (int i = 0; i < RETRY_LIMIT; i++) {
            final int docID = randomDocID();
            try {
                db.run(txn -> {
                    Utils.trace(txn, "addDocument(%d)", docID);
                    addDocument(txn, docID, doc).join();
                    txn.mutate(MutationType.ADD, FDBAccess.numDocsKey(index), ONE);
                    return null;
                });
                return docID;
            } catch (final CompletionException e) {
                if (e.getCause() instanceof DocIdCollisionException) {
                    // Try again.
                    continue;
                }
                throw e;
            }
        }
        throw new DocIdCollisionException();
    }

    public int[] addDocuments(final Document... docs) throws IOException {
        for (int i = 0; i < RETRY_LIMIT; i++) {
            final int[] ids = generateUniqueIDs(docs);
            final Collection<CompletableFuture<Void>> futures = new HashSet<CompletableFuture<Void>>();
            try {
                db.run(txn -> {
                    Utils.trace(txn, "addDocuments(%d)", docs.length);
                    for (int j = 0; j < docs.length; j++) {
                        final CompletableFuture<Void> f = addDocument(txn, ids[j], docs[j]);
                        futures.add(f);
                    }
                    txn.mutate(MutationType.ADD, FDBAccess.numDocsKey(index), ByteArrayUtil.encodeInt(docs.length));
                    AsyncUtil.whenAll(futures);
                    return null;
                });
                return ids;
            } catch (final CompletionException e) {
                if (e.getCause() instanceof DocIdCollisionException) {
                    // Try again.
                    continue;
                }
                throw e;
            }
        }
        throw new DocIdCollisionException();
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

    private CompletableFuture<Void> addDocument(final Transaction txn, final int docID, final Document doc)
            throws DocIdCollisionException {
        final Undo undo = new Undo();

        final CompletableFuture<byte[]> future = docIDFuture(txn, docID);
        txn.set(FDBAccess.docIDKey(index, docID), EMPTY);
        undo.clear(FDBAccess.docIDKey(index, docID));
        undo.mutate(MutationType.ADD, FDBAccess.numDocsKey(index), NEGATIVE_ONE);
        undo.mutate(MutationType.COMPARE_AND_CLEAR, FDBAccess.numDocsKey(index), ZERO);

        for (final IndexableField field : doc) {
            try {
                indexField(txn, docID, field, undo);
            } catch (final IOException e) {
                throw new CompletionException(e);
            }
        }
        undo.save(txn, FDBAccess.undoSpace(index, docID));
        return future.thenAccept(value -> {
            if (future.join() != null) {
                throw new DocIdCollisionException(docID);
            }
        });
    }

    private CompletableFuture<byte[]> docIDFuture(final Transaction txn, final int docID) {
        return txn.get(FDBAccess.docIDKey(index, docID));
    }

    public void deleteDocuments(final Term... terms) {
        db.run(txn -> {
            Utils.trace(txn, "deleteDocument(%s)", Arrays.toString(terms));
            for (final Term term : terms) {
                deleteDocuments(txn, term);
            }
            return null;
        });
    }

    public int updateDocument(final Term term, final Document doc) throws IOException {
        for (int i = 0; i < RETRY_LIMIT; i++) {
            final int docID = randomDocID();
            try {
                return db.run(txn -> {
                    Utils.trace(txn, "updateDocument(%s)", term);
                    deleteDocuments(txn, term);
                    addDocument(txn, docID, doc).join();
                    txn.mutate(MutationType.ADD, FDBAccess.numDocsKey(index), ONE);
                    return docID;
                });
            } catch (final CompletionException e) {
                if (e.getCause() instanceof DocIdCollisionException) {
                    // Try again.
                    continue;
                }
                throw e;
            }
        }
        throw new DocIdCollisionException();
    }

    private void deleteDocuments(final Transaction txn, final Term term) {
        final Subspace postings = FDBAccess.postingsMetaSubspace(index, term.field(), term.bytes());
        txn.getRange(postings.range()).forEach(kv -> {
            final int docID = (int) postings.unpack(kv.getKey()).getLong(0);
            deleteDocument(txn, docID);
        });
    }

    private void deleteDocument(final Transaction txn, final int docID) {
        final Undo undo = new Undo();
        undo.load(txn, FDBAccess.undoSpace(index, docID));
        undo.run(txn);
        txn.clear(FDBAccess.undoSpace(index, docID).range());
    }

    private void indexField(final Transaction txn, final int docID, final IndexableField field, final Undo undo)
            throws IOException {
        final String fieldName = field.name();
        final IndexableFieldType fieldType = field.fieldType();

        if (fieldType.indexOptions() != IndexOptions.NONE) {
            indexInvertedField(txn, docID, fieldName, field, undo);
        }

        if (fieldType.stored()) {
            indexStoredField(txn, docID, fieldName, field, undo);
        }

        final DocValuesType dvType = fieldType.docValuesType();
        if (dvType != DocValuesType.NONE) {
            indexDocValue(txn, dvType, docID, fieldName, field, undo);
        }

        if (fieldType.pointDataDimensionCount() > 0) {
            indexPoint(txn, docID, fieldName, field, undo);
        }

        txn.mutate(MutationType.ADD, FDBAccess.sumDocFreqKey(index, fieldName), ONE);
        undo.mutate(MutationType.ADD, FDBAccess.sumDocFreqKey(index, fieldName), NEGATIVE_ONE);
        undo.mutate(MutationType.COMPARE_AND_CLEAR, FDBAccess.sumDocFreqKey(index, fieldName), ZERO);
    }

    private void indexInvertedField(
            final Transaction txn,
            final int docID,
            final String fieldName,
            final IndexableField field,
            final Undo undo) throws IOException {

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

                if (!seen.contains(term)) {
                    txn.mutate(MutationType.ADD, FDBAccess.docFreqKey(index, fieldName, term), ONE);
                    undo.mutate(MutationType.ADD, FDBAccess.docFreqKey(index, fieldName, term), NEGATIVE_ONE);
                    undo.mutate(MutationType.COMPARE_AND_CLEAR, FDBAccess.docFreqKey(index, fieldName, term), ZERO);
                    seen.add(term);
                }
                txn.mutate(
                        MutationType.ADD,
                        FDBAccess.totalTermFreqKey(index, fieldName, term),
                        ByteArrayUtil.encodeInt(termFreq));
                undo.mutate(
                        MutationType.ADD,
                        FDBAccess.totalTermFreqKey(index, fieldName, term),
                        ByteArrayUtil.encodeInt(-termFreq));
                undo.mutate(MutationType.COMPARE_AND_CLEAR, FDBAccess.totalTermFreqKey(index, fieldName, term), ZERO);

                final byte[] postingsKey = FDBAccess.postingsPositionKey(index, fieldName, term, docID, pos);
                final byte[] postingsValue = FDBAccess.postingsValue(startOffset, endOffset, payload);
                txn.set(postingsKey, postingsValue);
                undo.clear(postingsKey);
                pos += posIncr;
                length++;
            }
            stream.end();

            if (!termFreqs.isEmpty()) {
                termFreqs.forEach((k, v) -> {
                    final byte[] key = FDBAccess.postingsMetaKey(index, fieldName, k, docID);
                    txn.set(key, ByteArrayUtil.encodeInt(v));
                    undo.clear(key);
                });

                txn.set(FDBAccess.normKey(index, fieldName, docID), ByteArrayUtil.encodeInt(length));
                undo.clear(FDBAccess.normKey(index, fieldName, docID));

                txn.mutate(MutationType.ADD, FDBAccess.docCountKey(index, fieldName), ONE);
                undo.mutate(MutationType.ADD, FDBAccess.docCountKey(index, fieldName), NEGATIVE_ONE);
                undo.mutate(MutationType.COMPARE_AND_CLEAR, FDBAccess.docCountKey(index, fieldName), ZERO);

                txn.mutate(
                        MutationType.ADD,
                        FDBAccess.sumTotalTermFreqKey(index, fieldName),
                        ByteArrayUtil.encodeInt(length));
                undo.mutate(
                        MutationType.ADD,
                        FDBAccess.sumTotalTermFreqKey(index, fieldName),
                        ByteArrayUtil.encodeInt(-length));
                undo.mutate(MutationType.COMPARE_AND_CLEAR, FDBAccess.sumTotalTermFreqKey(index, fieldName), ZERO);
            }
        }
    }

    private void indexStoredField(
            final Transaction txn,
            final int docID,
            final String fieldName,
            final IndexableField field,
            final Undo undo) {
        final byte[] key = FDBAccess.storedKey(index, docID, fieldName);
        final byte[] value = FDBAccess.storedValue(field);
        txn.set(key, value);
        undo.clear(key);
    }

    private void indexDocValue(
            final Transaction txn,
            final DocValuesType dvType,
            final int docID,
            final String fieldName,
            final IndexableField field,
            final Undo undo) {
        switch (dvType) {
        case NUMERIC: {
            final byte[] key = FDBAccess.numericDocValuesKey(index, fieldName, docID);
            txn.set(key, ByteArrayUtil.encodeInt(field.numericValue().longValue()));
            undo.clear(key);
            break;
        }
        case BINARY: {
            final byte[] key = FDBAccess.binaryDocValuesKey(index, fieldName, docID);
            txn.set(key, Utils.toBytes(field.binaryValue()));
            undo.clear(key);
            break;
        }
        default:
            throw new IllegalArgumentException("DocValue of type '" + dvType + "' not supported");
        }
    }

    private void indexPoint(
            final Transaction txn,
            final int docID,
            final String fieldName,
            final IndexableField field,
            final Undo undo) {
        throw new IllegalArgumentException("Points not supported");
    }

    private int randomDocID() {
        return random.nextInt(DocIdSetIterator.NO_MORE_DOCS);
    }

}

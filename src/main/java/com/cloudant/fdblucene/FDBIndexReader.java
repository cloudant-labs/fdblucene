package com.cloudant.fdblucene;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.CompletionException;
import java.util.function.Consumer;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.LeafMetaData;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.Version;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.TransactionContext;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;

public final class FDBIndexReader extends LeafReader {

    private static final LeafMetaData LEAF_META_DATA = new LeafMetaData(8, Version.LUCENE_8_1_0, null);

    private TransactionContext txc;
    private final Subspace index;

    public FDBIndexReader(final TransactionContext txc, final Subspace indexSubspace) {
        this.txc = txc;
        this.index = indexSubspace;
    }

    @Override
    public void checkIntegrity() throws IOException {
        // No-op.
    }

    @Override
    public void document(final int docID, final StoredFieldVisitor visitor) throws IOException {
        final Range range = FDBAccess.storedRange(index, docID);
        txc.run(txn -> {
            txn.getRange(range).forEach(new Consumer<KeyValue>() {

                @Override
                public void accept(final KeyValue kv) {
                    final Tuple keyTuple = index.unpack(kv.getKey());
                    final Tuple valueTuple = Tuple.fromBytes(kv.getValue());

                    final String fieldName = keyTuple.getString(2);

                    final String fieldType = valueTuple.getString(0);
                    final Object fieldValue = valueTuple.get(1);

                    final FieldInfo fieldInfo = new FieldInfo(fieldName, 1, false, true, false,
                            IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS, DocValuesType.NONE, -1L,
                            Collections.emptyMap(), 0, 0, 0, false);

                    try {
                        if ("b".equals(fieldType)) {
                            visitor.binaryField(fieldInfo, (byte[]) fieldValue);
                        } else if ("d".equals(fieldType)) {
                            visitor.doubleField(fieldInfo, (Double) fieldValue);
                        } else if ("f".equals(fieldType)) {
                            visitor.floatField(fieldInfo, (Float) fieldValue);
                        } else if ("i".equals(fieldType)) {
                            visitor.intField(fieldInfo, (Integer) fieldValue);
                        } else if ("l".equals(fieldType)) {
                            visitor.longField(fieldInfo, (Long) fieldValue);
                        } else if ("s".equals(fieldType)) {
                            visitor.stringField(fieldInfo, (byte[]) fieldValue);
                        }
                    } catch (final IOException e) {
                        throw new CompletionException(e);
                    }
                }
            });
            return null;
        });

    }

    @Override
    public BinaryDocValues getBinaryDocValues(final String field) throws IOException {
        throw new UnsupportedOperationException("getBinaryDocValues not supported.");
    }

    @Override
    public CacheHelper getCoreCacheHelper() {
        return null;
    }

    @Override
    public FieldInfos getFieldInfos() {
        throw new UnsupportedOperationException("getFieldInfos not supported.");
    }

    @Override
    public Bits getLiveDocs() {
        return null; // We'll never return a docID for a deleted document.
    }

    @Override
    public LeafMetaData getMetaData() {
        return LEAF_META_DATA;
    }

    @Override
    public NumericDocValues getNormValues(final String field) throws IOException {
        return new FDBNumericDocValues(txc, index, field) {

            @Override
            protected Subspace valueSubspace(Subspace index, String fieldName) {
                return FDBAccess.normSubspace(index, fieldName);
            }

        };
    }

    @Override
    public NumericDocValues getNumericDocValues(final String field) throws IOException {
        return new FDBNumericDocValues(txc, index, field) {

            @Override
            protected Subspace valueSubspace(final Subspace index, final String fieldName) {
                return FDBAccess.numericDocValuesSubspace(index, fieldName);
            }

        };
    }

    @Override
    public PointValues getPointValues(final String field) throws IOException {
        throw new UnsupportedOperationException("getPointValues not supported.");
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
        return null;
    }

    @Override
    public SortedDocValues getSortedDocValues(final String field) throws IOException {
        throw new UnsupportedOperationException("getSortedDocValues not supported.");
    }

    @Override
    public SortedNumericDocValues getSortedNumericDocValues(final String field) throws IOException {
        throw new UnsupportedOperationException("getSortedNumericDocValues not supported.");
    }

    @Override
    public SortedSetDocValues getSortedSetDocValues(final String field) throws IOException {
        throw new UnsupportedOperationException("getSortedSetDocValues not supported.");
    }

    @Override
    public Fields getTermVectors(final int docID) throws IOException {
        throw new UnsupportedOperationException("getTermVectors not supported.");
    }

    @Override
    public int maxDoc() {
        return DocIdSetIterator.NO_MORE_DOCS - 1;
    }

    @Override
    public int numDocs() {
        throw new UnsupportedOperationException("numDocs not supported.");
    }

    @Override
    public Terms terms(final String field) throws IOException {
        return new FDBTerms(txc, index, field);
    }

    @Override
    protected void doClose() throws IOException {
        // No-op.
    }

}

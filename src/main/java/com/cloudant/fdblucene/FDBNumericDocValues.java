package com.cloudant.fdblucene;

import java.io.IOException;
import java.util.List;

import org.apache.lucene.index.NumericDocValues;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.TransactionContext;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;

abstract class FDBNumericDocValues extends NumericDocValues {

    private final TransactionContext txc;
    private final Subspace index;
    private final String fieldName;

    private int docID = -1;
    private long value;

    public FDBNumericDocValues(final TransactionContext txc, final Subspace index, final String fieldName) {
        this.txc = txc;
        this.index = index;
        this.fieldName = fieldName;
    }

    protected abstract Subspace valueSubspace(final Subspace index, final String fieldName);

    @Override
    public final long longValue() throws IOException {
        return value;
    }

    @Override
    public final boolean advanceExact(final int target) throws IOException {
        final Subspace subspace = valueSubspace(index, fieldName);
        final byte[] begin = subspace.pack(target);
        final byte[] end = subspace.range().end;

        return txc.run(txn -> {
            final List<KeyValue> list = txn.getRange(begin, end, 1).asList().join();
            if (list.isEmpty()) {
                return false;
            }
            final KeyValue kv = list.get(0);
            this.docID = (int) subspace.unpack(kv.getKey()).getLong(0);
            this.value = ByteArrayUtil.decodeInt(kv.getValue());
            return this.docID == target;
        });
    }

    @Override
    public final int docID() {
        return docID;
    }

    @Override
    public final int nextDoc() throws IOException {
        throw new UnsupportedOperationException("nextDoc not supported.");
    }

    @Override
    public final int advance(int target) throws IOException {
        throw new UnsupportedOperationException("advance not supported.");
    }

    @Override
    public final long cost() {
        return 1L;
    }

}

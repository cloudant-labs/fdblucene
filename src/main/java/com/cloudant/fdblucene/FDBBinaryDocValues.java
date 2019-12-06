package com.cloudant.fdblucene;

import java.io.IOException;
import java.util.List;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.util.BytesRef;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.TransactionContext;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;

public final class FDBBinaryDocValues extends BinaryDocValues {

    private final TransactionContext txc;
    private final Subspace index;
    private final String fieldName;
    
    private int docID = -1;
    private BytesRef value;
    
    public FDBBinaryDocValues(final TransactionContext txc, final Subspace index, final String fieldName) {
        this.txc = txc;
        this.index = index;
        this.fieldName = fieldName;
    }

    @Override
    public BytesRef binaryValue() throws IOException {
        return value;
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
        final Subspace subspace = FDBAccess.binaryDocValuesSubspace(index, fieldName);
        final byte[] begin = subspace.pack(target);
        final byte[] end = subspace.range().end;

        return txc.run(txn -> {
            final List<KeyValue> list = txn.getRange(begin, end, 1).asList().join();
            if (list.isEmpty()) {
                return false;
            }
            final KeyValue kv = list.get(0);
            this.docID = (int) subspace.unpack(kv.getKey()).getLong(0);
            this.value = new BytesRef(kv.getValue());
            return this.docID == target;
        });
    }

    @Override
    public int docID() {
        return docID;
    }


    @Override
    public int nextDoc() throws IOException {
        throw new UnsupportedOperationException("nextDoc not supported.");
    }

    @Override
    public int advance(int target) throws IOException {
        throw new UnsupportedOperationException("advance not supported.");
    }

    @Override
    public long cost() {
        return 1L;
    }
}

package com.cloudant.fdblucene;

import java.io.IOException;

import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;

import com.apple.foundationdb.TransactionContext;
import com.apple.foundationdb.subspace.Subspace;

final class FDBTerms extends Terms {

    private final TransactionContext txc;
    private final Subspace index;
    private final String fieldName;

    public FDBTerms(final TransactionContext txc, final Subspace index, final String fieldName) {
        this.txc = txc;
        this.index = index;
        this.fieldName = fieldName;
    }

    @Override
    public TermsEnum iterator() throws IOException {
        return new FDBTermsEnum(txc, index, fieldName);
    }

    @Override
    public long size() throws IOException {
        throw new UnsupportedOperationException("size not supported.");
    }

    @Override
    public long getSumTotalTermFreq() throws IOException {
        final byte[] key = FDBAccess.sumTotalTermFreqKey(index, fieldName);
        return Utils.getOrDefault(txc, key, 0);
    }

    @Override
    public long getSumDocFreq() throws IOException {
        final byte[] key = FDBAccess.sumDocFreqKey(index, fieldName);
        return Utils.getOrDefault(txc, key, 0);
    }

    @Override
    public int getDocCount() throws IOException {
        final byte[] key = FDBAccess.docCountKey(index, fieldName);
        return Utils.getOrDefault(txc, key, 0);
    }

    @Override
    public boolean hasFreqs() {
        throw new UnsupportedOperationException("hasFreqs not supported.");
    }

    @Override
    public boolean hasOffsets() {
        throw new UnsupportedOperationException("hasOffsets not supported.");
    }

    @Override
    public boolean hasPositions() {
        return true;
    }

    @Override
    public boolean hasPayloads() {
        throw new UnsupportedOperationException("hasPayloads not supported.");
    }

}

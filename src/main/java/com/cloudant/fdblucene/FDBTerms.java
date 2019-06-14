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

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
import java.util.concurrent.CompletableFuture;

import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SlowImpactsEnum;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;

final class FDBTermsEnum extends TermsEnum {

    private static final TermState TERM_STATE = new TermState() {

        @Override
        public void copyFrom(TermState other) {
            // no-op.
        }

    };

    private final Transaction txn;
    private final Subspace index;
    private final String fieldName;

    private BytesRef term;
    private int docFreq;
    private int totalTermFreq;

    public FDBTermsEnum(final Transaction txn, final Subspace index, final String fieldName) {
        this.txn = txn;
        this.index = index;
        this.fieldName = fieldName;
    }

    @Override
    public BytesRef next() throws IOException {
        throw new UnsupportedOperationException("next not supported.");
    }

    @Override
    public AttributeSource attributes() {
        throw new UnsupportedOperationException("attributes not supported.");
    }

    @Override
    public boolean seekExact(final BytesRef text) throws IOException {
        final byte[] docFreqKey = FDBAccess.docFreqKey(index, fieldName, text);
        final CompletableFuture<byte[]> docFreqFuture = txn.get(docFreqKey);

        final byte[] totalTermFreqKey = FDBAccess.totalTermFreqKey(index, fieldName, text);
        final CompletableFuture<byte[]> totalTermFreqFuture = txn.get(totalTermFreqKey);

        final byte[] docFreq = docFreqFuture.join();
        final byte[] totalTermFreq = totalTermFreqFuture.join();

        if (docFreq != null) {
            this.term = text;
            this.docFreq = (int) ByteArrayUtil.decodeInt(docFreq);
            this.totalTermFreq = (int) ByteArrayUtil.decodeInt(totalTermFreq);
        }

        return docFreq != null;
    }

    @Override
    public SeekStatus seekCeil(BytesRef text) throws IOException {
        FDBAccess.docFreqKey(index, fieldName, text);
        FDBAccess.totalTermFreqKey(index, fieldName, text);

        final byte[] docFreqKey = FDBAccess.docFreqKey(index, fieldName, text);
        final Range docFreqRange = FDBAccess.docFreqRange(index, fieldName);
        return txn.getRange(docFreqKey, docFreqRange.end, 1).asList().thenApply(result -> {
            if (result.isEmpty()) {
                return SeekStatus.END;
            }
            final KeyValue kv = result.get(0);
            final Tuple keyTuple = index.unpack(kv.getKey());
            this.term = new BytesRef(keyTuple.getBytes(2));
            this.docFreq = (int) ByteArrayUtil.decodeInt(kv.getValue());
            final byte[] totalTermFreqKey = FDBAccess.totalTermFreqKey(index, fieldName, term);
            final byte[] totalTermFreq = txn.get(totalTermFreqKey).join();
            this.totalTermFreq = (int) ByteArrayUtil.decodeInt(totalTermFreq);
            if (term.bytesEquals(text)) {
                return SeekStatus.FOUND;
            } else {
                return SeekStatus.NOT_FOUND;
            }
        }).join();
    }

    @Override
    public void seekExact(long ord) throws IOException {
        throw new UnsupportedOperationException("seekExact not supported.");
    }

    @Override
    public void seekExact(final BytesRef term, final TermState state) throws IOException {
        assert state == TERM_STATE;
        seekExact(term);
    }

    @Override
    public BytesRef term() throws IOException {
        return term;
    }

    @Override
    public long ord() throws IOException {
        throw new UnsupportedOperationException("ord not supported.");
    }

    @Override
    public int docFreq() throws IOException {
        return docFreq;
    }

    @Override
    public long totalTermFreq() throws IOException {
        return totalTermFreq;
    }

    @Override
    public PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {
        return new FDBPostingsEnum(txn, index, fieldName, term);
    }

    @Override
    public ImpactsEnum impacts(int flags) throws IOException {
        return new SlowImpactsEnum(postings(null, flags));
    }

    @Override
    public TermState termState() throws IOException {
        return TERM_STATE;
    }

}

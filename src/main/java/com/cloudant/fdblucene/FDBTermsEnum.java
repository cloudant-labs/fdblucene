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

import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SlowImpactsEnum;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.TransactionContext;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;

final class FDBTermsEnum extends TermsEnum {

    private static final TermState TERM_STATE = new TermState() {

        @Override
        public void copyFrom(TermState other) {
            // no-op.
        }

    };

    private final TransactionContext txc;
    private final Subspace index;
    private final String fieldName;

    private BytesRef term;
    private int docFreq;
    private int totalTermFreq;

    public FDBTermsEnum(final TransactionContext txc, final Subspace index, final String fieldName) {
        this.txc = txc;
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
        final byte[] key = FDBAccess.termKey(index, fieldName, text);
        final byte[] value = txc.run(txn -> {
            return txn.get(key);
        }).join();

        if (value != null) {
            this.term = text;
            updateState(value);
        }

        return value != null;
    }

    @Override
    public SeekStatus seekCeil(BytesRef text) throws IOException {
        final byte[] key = FDBAccess.termKey(index, fieldName, text);
        final Range fieldRange = FDBAccess.fieldRange(index, fieldName);
        return txc.run(txn -> {
            return txn.getRange(key, fieldRange.end, 1).asList().thenApply(result -> {
                if (result.isEmpty()) {
                    return SeekStatus.END;
                }
                final KeyValue kv = result.get(0);
                final Tuple keyTuple = index.unpack(kv.getKey());
                this.term = new BytesRef(keyTuple.getBytes(2));
                updateState(kv.getValue());
                if (term.bytesEquals(text)) {
                    return SeekStatus.FOUND;
                } else {
                    return SeekStatus.NOT_FOUND;
                }
            });
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
        return new FDBPostingsEnum(txc, index, fieldName, term);
    }

    @Override
    public ImpactsEnum impacts(int flags) throws IOException {
        return new SlowImpactsEnum(postings(null, flags));
    }

    @Override
    public TermState termState() throws IOException {
        return TERM_STATE;
    }

    private void updateState(final byte[] value) {
        this.docFreq = Utils.decodeInt(value, 0);
        this.totalTermFreq = Utils.decodeInt(value, 8);
    }

}

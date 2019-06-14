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

import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.TransactionContext;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;

class FDBPostingsEnum extends PostingsEnum {

    private final TransactionContext txc;
    private final Subspace index;
    private final String fieldName;
    private final BytesRef term;

    private int freq = 1;
    private int pos = -1;
    private int startOffset = -1;
    private int endOffset = -1;
    private BytesRef payload = null;
    private int docID = -1;

    public FDBPostingsEnum(final TransactionContext txc, final Subspace index, final String fieldName,
            final BytesRef term) {
        this.txc = txc;
        this.index = index;
        this.fieldName = fieldName;
        this.term = BytesRef.deepCopyOf(term);
    }

    @Override
    public int freq() throws IOException {
        return freq;
    }

    @Override
    public int nextPosition() throws IOException {
        final Subspace postingsSubspace = FDBAccess.postingsSubspace(index, fieldName, term);
        final byte[] begin = postingsSubspace.pack(Tuple.from(this.docID, this.pos + 1));
        final byte[] end = postingsSubspace.range().end;

        return txc.run(txn -> {
            return txn.getRange(begin, end, 1).asList().thenApply(result -> {
                if (result.isEmpty()) {
                    throw new Error("nextPosition called too many times");
                }
                final KeyValue kv = result.get(0);
                final Tuple kt = postingsSubspace.unpack(kv.getKey());
                final Tuple vt = Tuple.fromBytes(kv.getValue());
                this.pos = (int) kt.getLong(1);
                this.startOffset = (int) vt.getLong(0);
                this.endOffset = (int) vt.getLong(1);
                final byte[] payload = vt.getBytes(2);
                if (payload == null) {
                    this.payload = null;
                } else {
                    this.payload = new BytesRef(payload);
                }
                return this.pos;
            });
        }).join();
    }

    @Override
    public int startOffset() throws IOException {
        return startOffset;
    }

    @Override
    public int endOffset() throws IOException {
        return endOffset;
    }

    @Override
    public BytesRef getPayload() throws IOException {
        return payload;
    }

    @Override
    public int docID() {
        return docID;
    }

    @Override
    public int nextDoc() throws IOException {
        return advance(docID + 1);
    }

    @Override
    public int advance(final int target) throws IOException {
        final Subspace postingsSubspace = FDBAccess.postingsSubspace(index, fieldName, term);
        final byte[] begin = postingsSubspace.pack(target);
        final byte[] end = postingsSubspace.range().end;

        return txc.run(txn -> {
            return txn.getRange(begin, end, 1).asList().thenApply(result -> {
                if (result.isEmpty()) {
                    this.docID = DocIdSetIterator.NO_MORE_DOCS;
                    return DocIdSetIterator.NO_MORE_DOCS;
                }

                final KeyValue kv = result.get(0);
                final Tuple kt = postingsSubspace.unpack(kv.getKey());
                this.docID = (int) kt.getLong(0);
                this.freq = (int) ByteArrayUtil.decodeInt(kv.getValue());
                this.pos = -1;
                this.startOffset = -1;
                this.endOffset = -1;
                this.payload = null;
                return this.docID;
            });
        }).join();
    }

    @Override
    public long cost() {
        return 1L;
    }

    public String toString() {
        return term.utf8ToString();
    }

}

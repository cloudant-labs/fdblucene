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

import java.nio.charset.StandardCharsets;

import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;

import com.apple.foundationdb.Range;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;

final class FDBAccess {

    static byte[] docIDKey(final Subspace index, final int docID) {
        return index.pack(Tuple.from("d", docID));
    }

    static byte[] normKey(final Subspace index, final String fieldName, final int docID) {
        return index.pack(Tuple.from("nv", fieldName, docID));
    }

    static Subspace normSubspace(final Subspace index, final String fieldName) {
        final Tuple t = Tuple.from("nv", fieldName);
        return index.get(t);
    }

    static byte[] numericDocValuesKey(final Subspace index, final String fieldName, final int docID) {
        return index.pack(Tuple.from("ndv", fieldName, docID));
    }

    static Subspace numericDocValuesSubspace(final Subspace index, final String fieldName) {
        final Tuple t = Tuple.from("ndv", fieldName);
        return index.get(t);
    }

    static byte[] binaryDocValuesKey(final Subspace index, final String fieldName, final int docID) {
        return index.pack(Tuple.from("bdv", fieldName, docID));
    }

    static Subspace binaryDocValuesSubspace(final Subspace index, final String fieldName) {
        final Tuple t = Tuple.from("bdv", fieldName);
        return index.get(t);
    }

    static Range docFreqRange(final Subspace index, final String fieldName) {
        return index.range(Tuple.from("df", fieldName));
    }

    static byte[] docFreqKey(final Subspace index, final String fieldName, final BytesRef term) {
        return index.pack(Tuple.from("df", fieldName, Utils.toBytes(term)));
    }

    static byte[] totalTermFreqKey(final Subspace index, final String fieldName, final BytesRef term) {
        return index.pack(Tuple.from("ttf", fieldName, Utils.toBytes(term)));
    }

    static byte[] postingsMetaKey(final Subspace index, final String fieldName, final BytesRef term, final int docID) {
        final Tuple t = Tuple.from("pm", fieldName, Utils.toBytes(term), docID);
        return index.pack(t);
    }

    static byte[] postingsPositionKey(
            final Subspace index,
            final String fieldName,
            final BytesRef term,
            final int docID,
            final int pos) {
        final Tuple t = Tuple.from("pp", fieldName, Utils.toBytes(term), docID, pos);
        return index.pack(t);
    }

    static Subspace postingsMetaSubspace(final Subspace index, final String fieldName, final BytesRef term) {
        final Tuple t = Tuple.from("pm", fieldName, Utils.toBytes(term));
        return index.get(t);
    }

    static Subspace postingsPositionSubspace(
            final Subspace index,
            final String fieldName,
            final BytesRef term,
            final int docID) {
        final Tuple t = Tuple.from("pp", fieldName, Utils.toBytes(term), docID);
        return index.get(t);
    }

    static byte[] postingsValue(final int startOffset, final int endOffset, final BytesRef payload) {
        return Tuple.from(startOffset, endOffset, payload == null ? null : Utils.toBytes(payload)).pack();
    }

    static Range storedRange(final Subspace index, final int docID) {
        return index.range(Tuple.from("s", docID));
    }

    static byte[] storedKey(final Subspace index, final int docID, final String fieldName) {
        final Tuple t = Tuple.from("s", docID, fieldName);
        return index.pack(t);
    }

    static byte[] storedValue(final IndexableField field) {
        Number number = field.numericValue();
        if (number != null) {
            if (number instanceof Byte || number instanceof Short || number instanceof Integer) {
                return Tuple.from("i", number).pack();
            } else if (number instanceof Long) {
                return Tuple.from("l", number).pack();
            } else if (number instanceof Float) {
                return Tuple.from("f", number).pack();
            } else if (number instanceof Double) {
                return Tuple.from("d", number).pack();
            } else {
                throw new IllegalArgumentException("cannot store numeric type " + number.getClass());
            }
        }

        final BytesRef ref = field.binaryValue();
        if (ref != null) {
            return Tuple.from("b", Utils.toBytes(ref)).pack();
        }

        final String string = field.stringValue();
        return Tuple.from("s", string.getBytes(StandardCharsets.UTF_8)).pack();
    }

    static byte[] numDocsKey(final Subspace index) {
        return index.pack(Tuple.from("i", "nd"));
    }

    static byte[] docCountKey(final Subspace index, final String fieldName) {
        return index.pack(Tuple.from("f", fieldName, "dc"));
    }

    static byte[] sumDocFreqKey(final Subspace index, final String fieldName) {
        return index.pack(Tuple.from("f", fieldName, "sdf"));
    }

    static byte[] sumTotalTermFreqKey(final Subspace index, final String fieldName) {
        return index.pack(Tuple.from("f", fieldName, "sttf"));
    }

    static Subspace undoSpace(final Subspace index, final int docID) {
        return index.get(Tuple.from("undo", docID));
    }

}

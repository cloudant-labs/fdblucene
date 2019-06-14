/*******************************************************************************
 * Copyright 2019 IBM Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *******************************************************************************/
package com.cloudant.fdblucene;

import org.apache.lucene.util.BytesRef;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.TransactionContext;
import com.apple.foundationdb.tuple.ByteArrayUtil;

class Utils {

    static void trace(final Transaction txn, final String format, final Object... args) {
        if (System.getenv("FDB_NETWORK_OPTION_TRACE_ENABLE") != null) {
            final String str = String.format(format, args);
            txn.options().setDebugTransactionIdentifier(str);
            txn.options().setLogTransaction();
        }
    }

    static byte[] toBytes(final BytesRef ref) {
        final byte[] result = new byte[ref.length];
        System.arraycopy(ref.bytes, ref.offset, result, 0, ref.length);
        return result;
    }

    static void encodeInt(final byte[] arr, final int offset, final int v) {
        arr[offset] = (byte) (0xff & v);
        arr[offset + 1] = (byte) (0xff & (v >> 8));
        arr[offset + 2] = (byte) (0xff & (v >> 16));
        arr[offset + 3] = (byte) (0xff & (v >> 24));
        arr[offset + 4] = 0;
        arr[offset + 5] = 0;
        arr[offset + 6] = 0;
        arr[offset + 7] = 0;
    }

    static int decodeInt(final byte[] value, final int offset) {
        return ((value[offset] & 0xff) | ((value[offset + 1] & 0xff) << 8) | ((value[offset + 2] & 0xff) << 16)
                | (value[offset + 3] & 0xff) << 24);
    }

    static int getOrDefault(final TransactionContext txc, final byte[] key, final int defaultValue) {
        final byte[] value = txc.read(txn -> {
            return txn.get(key);
        }).join();
        if (value == null) {
            return defaultValue;
        } else {
            return (int) ByteArrayUtil.decodeInt(value);
        }
    }

}

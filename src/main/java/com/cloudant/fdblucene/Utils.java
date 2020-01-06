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

import java.nio.ByteBuffer;
import java.util.UUID;

import org.apache.lucene.util.BytesRef;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.TransactionContext;
import com.apple.foundationdb.tuple.ByteArrayUtil;

class Utils {

    static void trace(final Transaction txn, final String format, final Object... args) {
        if (System.getenv("FDB_NETWORK_OPTION_TRACE_ENABLE") != null) {
            final String str = String.format(format, args);
            txn.options().setTransactionLoggingEnable(str);
        }
    }

    static byte[] toBytes(final BytesRef ref) {
        final byte[] result = new byte[ref.length];
        System.arraycopy(ref.bytes, ref.offset, result, 0, ref.length);
        return result;
    }

    static byte[] toBytes(final UUID uuid) {
        final ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
        bb.putLong(uuid.getMostSignificantBits());
        bb.putLong(uuid.getLeastSignificantBits());
        return bb.array();
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

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
import java.security.GeneralSecurityException;
import java.util.UUID;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;

import org.apache.lucene.util.BytesRef;

import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.TransactionContext;
import com.apple.foundationdb.tuple.ByteArrayUtil;

class Utils {

    static void trace(final ReadTransaction txn, final String format, final Object... args) {
        if (System.getenv("FDB_NETWORK_OPTION_TRACE_ENABLE") != null) {
            final String str = String.format(format, args);
            txn.options().setLogTransaction();
            txn.options().setDebugTransactionIdentifier(str);
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

    static byte[] encrypt(final SecretKey secretKey, final long pageNumber, final byte[] data)
            throws GeneralSecurityException {
        final Cipher result = Cipher.getInstance("AES_256/GCM/NoPadding");
        result.init(Cipher.ENCRYPT_MODE, secretKey, new GCMParameterSpec(128, iv(pageNumber)));
        return result.doFinal(data);
    }

    static byte[] decrypt(final SecretKey secretKey, final long pageNumber, final byte[] data)
            throws GeneralSecurityException {
        final Cipher result = Cipher.getInstance("AES_256/GCM/NoPadding");
        result.init(Cipher.DECRYPT_MODE, secretKey, new GCMParameterSpec(128, iv(pageNumber)));
        return result.doFinal(data);
    }

    private static byte[] iv(final long v) {
        final byte[] result = new byte[12];
        result[0] = (byte) (0xff & (v >> 56));
        result[1] = (byte) (0xff & (v >> 48));
        result[2] = (byte) (0xff & (v >> 40));
        result[3] = (byte) (0xff & (v >> 32));
        result[4] = (byte) (0xff & (v >> 24));
        result[5] = (byte) (0xff & (v >> 16));
        result[6] = (byte) (0xff & (v >> 8));
        result[7] = (byte) (0xff & v);
        return result;
    }

}

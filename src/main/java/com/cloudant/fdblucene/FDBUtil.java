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

import java.security.SecureRandom;
import java.util.Random;
import java.util.UUID;

import com.apple.foundationdb.tuple.Tuple;

final class FDBUtil {

    public static final Random RANDOM = new SecureRandom();

    static final int DEFAULT_PAGE_SIZE = 100_000;

    static final int DEFAULT_TXN_SIZE = 1_000_000;

    static int decodeInt(final byte[] v) {
        return (((v[0] & 0xff) << 24) | ((v[1] & 0xff) << 16) | ((v[2] & 0xff) << 8) | (v[3] & 0xff));
    }

    static long decodeLong(final byte[] v) {
        return (((long) (v[0] & 0xff) << 56) | ((long) (v[1] & 0xff) << 48) | ((long) (v[2] & 0xff) << 40)
                | ((long) (v[3] & 0xff) << 32) | ((long) (v[4] & 0xff) << 24) | ((long) (v[5] & 0xff) << 16)
                | ((long) (v[6] & 0xff) << 8) | (v[7] & 0xff));
    }

    static byte[] encodeInt(final int v) {
        final byte[] result = new byte[4];
        result[0] = (byte) (0xff & (v >> 24));
        result[1] = (byte) (0xff & (v >> 16));
        result[2] = (byte) (0xff & (v >> 8));
        result[3] = (byte) (0xff & v);
        return result;
    }

    static byte[] encodeLong(final long v) {
        final byte[] result = new byte[8];
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

    static int posToOffset(final long pos, final int pageSize) {
        return (int) (pos % pageSize);
    }

    static long posToPage(final long pos, final int pageSize) {
        return pos / pageSize;
    }

}

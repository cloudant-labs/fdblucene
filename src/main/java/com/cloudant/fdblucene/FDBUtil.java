package com.cloudant.fdblucene;

import java.security.SecureRandom;
import java.util.Random;

final class FDBUtil {

    public static final Random RANDOM = new SecureRandom();

    static final int PAGE_SIZE = 100_000;

    static final int TXN_SIZE = 1_000_000;

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

    static byte[] newPage() {
        return new byte[PAGE_SIZE];
    }

    static byte[] newTxnBuffer() {
        return new byte[TXN_SIZE];
    }

    static int posToOffset(final long pos) {
        return (int) (pos % PAGE_SIZE);
    }

    static long posToPage(final long pos) {
        return pos / PAGE_SIZE;
    }

}

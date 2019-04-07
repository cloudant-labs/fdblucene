package com.cloudant.fdblucene;

class FDBTestUtil {

    static byte[] testArray(final int size) {
        final byte[] result = new byte[size];
        for (int i = 0; i < result.length; i++) {
            result[i] = (byte) (i % 0x7f);
        }
        return result;
    }

}

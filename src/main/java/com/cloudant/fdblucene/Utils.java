package com.cloudant.fdblucene;

import java.io.IOException;
import java.util.concurrent.CompletionException;

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

    /**
     * Allow natural throwing of IOException from Lucene methods within an FDB
     * transaction.
     *
     * @throws IOException
     */
    static <T> T run(final TransactionContext txc, IOFunction<? super Transaction, T> retryable) throws IOException {
        try {
            return txc.run(txn -> {
                try {
                    return retryable.apply(txn);
                } catch (final IOException e) {
                    throw new CompletionException(e);
                }
            });
        } catch (final CompletionException e) {
            final Throwable cause = e.getCause();
            if (cause instanceof IOException) {
                throw (IOException) cause;
            }
            throw e;
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

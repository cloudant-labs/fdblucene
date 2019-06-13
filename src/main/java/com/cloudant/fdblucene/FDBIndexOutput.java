package com.cloudant.fdblucene;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.zip.CRC32;

import org.apache.lucene.store.IndexOutput;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.TransactionContext;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.subspace.Subspace;

public final class FDBIndexOutput extends IndexOutput {

    private static void flushTxnBuffer(
            final Subspace subspace,
            final Transaction txn,
            final byte[] txnBuffer,
            final int txnBufferOffset,
            final long pointer,
            final int pageSize) {
        final byte[] fullPage = new byte[pageSize];
        for (int i = 0; i < txnBufferOffset; i += pageSize) {
            final long pos = pointer - txnBufferOffset + i;
            final byte[] key = pageKey(subspace, pos, pageSize);
            final int flushSize = Math.min(pageSize, txnBufferOffset - i);
            final byte[] bufToFlush;
            if (flushSize == pageSize) {
                bufToFlush = fullPage;
            } else {
                bufToFlush = new byte[flushSize];
            }
            System.arraycopy(txnBuffer, i, bufToFlush, 0, flushSize);
            txn.options().setNextWriteNoWriteConflictRange();
            txn.set(key, bufToFlush);
        }
    }

    private static byte[] pageKey(final Subspace subspace, final long pos, final int byteSize) {
        final long currentPage = FDBUtil.posToPage(pos, byteSize);
        return subspace.pack(currentPage);
    }

    private final FDBDirectory dir;
    private final TransactionContext txc;
    private final Subspace subspace;
    private byte[] txnBuffer;

    private int txnBufferOffset;
    private final CRC32 crc;

    private CompletableFuture<Void> lastFlushFuture;
    private long pointer;

    private final ReadVersionCache readVersionCache;

    private final int pageSize;
    private final int txnSize;

    FDBIndexOutput(final FDBDirectory dir, final String resourceDescription, final String name,
            final TransactionContext txc, final Subspace subspace, final int pageSize, final int txnSize) {
        super(resourceDescription, name);
        this.dir = dir;
        this.txc = txc;
        this.subspace = subspace;
        this.readVersionCache = new ReadVersionCache();
        this.pageSize = pageSize;
        this.txnSize = txnSize;
        txnBuffer = new byte[txnSize];
        crc = new CRC32();
        lastFlushFuture = AsyncUtil.DONE;
        pointer = 0L;
    }

    @Override
    public void close() throws IOException {
        lastFlushFuture.join();
        txc.run(txn -> {
            readVersionCache.setReadVersion(txn);
            txn.options().setTransactionLoggingEnable(String.format("%s,out,close,%d", getName(), pointer));
            flushTxnBuffer(subspace, txn, txnBuffer, txnBufferOffset, pointer, pageSize);
            txn.options().setNextWriteNoWriteConflictRange();

            dir.setFileLength(txn, getName(), pointer);
            return null;
        });
    }

    @Override
    public long getChecksum() throws IOException {
        return crc.getValue();
    }

    @Override
    public long getFilePointer() {
        return pointer;
    }

    @Override
    public void writeByte(final byte b) throws IOException {
        txnBuffer[txnBufferOffset] = b;
        txnBufferOffset++;
        pointer++;
        crc.update(b);
        flushTxnBufferIfFull();
    }

    @Override
    public void writeBytes(final byte[] b, final int offset, final int length) throws IOException {
        int writeOffset = offset;
        int writeLength = length;
        while (writeLength > 0) {
            final int bytesToCopy = Math.min(writeLength, txnBuffer.length - txnBufferOffset);
            System.arraycopy(b, writeOffset, txnBuffer, txnBufferOffset, bytesToCopy);
            writeOffset += bytesToCopy;
            txnBufferOffset += bytesToCopy;
            pointer += bytesToCopy;
            writeLength -= bytesToCopy;
            flushTxnBufferIfFull();
        }
        crc.update(b, offset, length);
    }

    private void flushTxnBuffer() {
        lastFlushFuture.join();

        final byte[] txnBuffer = this.txnBuffer;
        final int txnBufferOffset = this.txnBufferOffset;

        this.txnBuffer = new byte[this.txnSize];
        this.txnBufferOffset = 0;

        lastFlushFuture = txc.runAsync(txn -> {
            readVersionCache.setReadVersion(txn);
            txn.options().setTransactionLoggingEnable(String.format("%s,out,flush,%d", getName(), pointer));
            flushTxnBuffer(subspace, txn, txnBuffer, txnBufferOffset, pointer, pageSize);
            return AsyncUtil.DONE;
        });
    }

    private void flushTxnBufferIfFull() {
        if (txnBufferOffset == txnBuffer.length) {
            flushTxnBuffer();
        }
    }

}

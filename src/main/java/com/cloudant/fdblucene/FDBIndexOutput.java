package com.cloudant.fdblucene;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.zip.CRC32;

import org.apache.lucene.store.IndexOutput;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.TransactionContext;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.directory.DirectorySubspace;

public final class FDBIndexOutput extends IndexOutput {

    private final TransactionContext txc;
    private final DirectorySubspace subdir;
    private final byte[] txnBuffer;
    private int txnBufferOffset;
    private final CRC32 crc;

    private CompletableFuture<Void> lastFlushFuture;
    private long pointer;

    private long readVersion;
    private long readVersionAt;

    public FDBIndexOutput(final String resourceDescription, final String name, final TransactionContext txc,
            final DirectorySubspace subdir) {
        super(resourceDescription, name);
        this.txc = txc;
        this.subdir = subdir;
        txnBuffer = FDBUtil.newTxnBuffer();
        crc = new CRC32();
        lastFlushFuture = AsyncUtil.DONE;
        pointer = 0L;
        readVersion = -1L;
        readVersionAt = System.currentTimeMillis();
    }

    @Override
    public void close() throws IOException {
        lastFlushFuture.join();
        txc.run(txn -> {
            setReadVersion(txn);
            txn.options().setTransactionLoggingEnable(String.format("%s,out,close,%d", getName(), pointer));
            flushTxnBuffer(txn);
            txn.options().setNextWriteNoWriteConflictRange();
            txn.set(subdir.pack("length"), FDBUtil.encodeLong(pointer));
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

    private void flushTxnBufferIfFull() {
        if (txnBufferOffset == txnBuffer.length) {
            flushTxnBuffer();
        }
    }

    private void flushTxnBuffer() {
        lastFlushFuture.join();
        lastFlushFuture = txc.runAsync(txn -> {
            setReadVersion(txn);
            txn.options().setTransactionLoggingEnable(String.format("%s,out,flush,%d", getName(), pointer));
            flushTxnBuffer(txn);
            return AsyncUtil.DONE;
        });
    }

    private void flushTxnBuffer(final Transaction txn) {
        final byte[] fullPage = FDBUtil.newPage();
        for (int i = 0; i < txnBufferOffset; i += FDBUtil.PAGE_SIZE) {
            final long pos = this.pointer - txnBufferOffset + i;
            final byte[] key = pageKey(pos);
            final int flushSize = Math.min(FDBUtil.PAGE_SIZE, txnBufferOffset - i);
            final byte[] bufToFlush;
            if (flushSize == FDBUtil.PAGE_SIZE) {
                bufToFlush = fullPage;
            } else {
                bufToFlush = new byte[flushSize];
            }
            System.arraycopy(txnBuffer, i, bufToFlush, 0, flushSize);
            txn.options().setNextWriteNoWriteConflictRange();
            txn.set(key, bufToFlush);
        }
        txnBufferOffset = 0;
    }

    private byte[] pageKey(final long pos) {
        final long currentPage = FDBUtil.posToPage(pos);
        return subdir.pack(currentPage);
    }

    private void setReadVersion(final Transaction txn) {
        final long now = System.currentTimeMillis();
        final long readVersionAge = now - this.readVersionAt;
        if (this.readVersion == -1L || readVersionAge > 150) {
            this.readVersion = txn.getReadVersion().join();
            this.readVersionAt = now;
        } else {
            txn.setReadVersion(readVersion);
        }
    }

}

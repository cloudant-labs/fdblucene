package com.cloudant.fdblucene;

import java.io.IOException;
import java.util.zip.CRC32;

import org.apache.lucene.store.IndexOutput;

import com.apple.foundationdb.TransactionContext;
import com.apple.foundationdb.directory.DirectorySubspace;

public final class FDBIndexOutput extends IndexOutput {

    private final TransactionContext txc;
    private final DirectorySubspace subdir;
    private final byte[] page;
    private int pageOffset;
    private final CRC32 crc;

    private long pointer;

    public FDBIndexOutput(final String resourceDescription, final String name, final TransactionContext txc,
            final DirectorySubspace subdir) {
        super(resourceDescription, name);
        this.txc = txc;
        this.subdir = subdir;
        page = FDBUtil.newPage();
        crc = new CRC32();
        pointer = 0L;
    }

    @Override
    public void close() throws IOException {
        txc.run(txn -> {
            if (pageOffset > 0) {
                final byte[] key = pageKey(this.pointer);
                final byte[] value = new byte[pageOffset];
                System.arraycopy(page, 0, value, 0, pageOffset);
                txn.set(key, value);
            }
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
        page[pageOffset] = b;
        pageOffset++;
        pointer++;
        crc.update(b);
        flushPageIfFull();
    }

    @Override
    public void writeBytes(final byte[] b, final int offset, final int length) throws IOException {
        int writeOffset = offset;
        int writeLength = length;
        while (writeLength > 0) {
            final int bytesToCopy = Math.min(writeLength, page.length - pageOffset);
            System.arraycopy(b, writeOffset, page, pageOffset, bytesToCopy);
            writeOffset += bytesToCopy;
            pageOffset += bytesToCopy;
            pointer += bytesToCopy;
            writeLength -= bytesToCopy;
            flushPageIfFull();
        }
        crc.update(b, offset, length);
    }

    private void flushPageIfFull() {
        if (pageOffset == page.length) {
            final byte[] key = pageKey(this.pointer - 1);
            txc.run(txn -> {
                txn.set(key, page);
                return null;
            });
            pageOffset = 0;
        }
    }

    private byte[] pageKey(final long pos) {
        final long currentPage = FDBUtil.posToPage(pos);
        return subdir.pack(currentPage);
    }

}

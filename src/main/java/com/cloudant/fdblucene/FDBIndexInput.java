package com.cloudant.fdblucene;

import java.io.EOFException;
import java.io.IOException;

import org.apache.lucene.store.IndexInput;

import com.apple.foundationdb.TransactionContext;
import com.apple.foundationdb.directory.DirectorySubspace;

public final class FDBIndexInput extends IndexInput {

    private final TransactionContext txc;
    private final DirectorySubspace subdir;
    private final long offset;
    private final long length;
    private byte[] page;
    private long pointer;

    public FDBIndexInput(final String resourceDescription, final TransactionContext txc, final DirectorySubspace subdir,
            final long offset, final long length) {
        super(resourceDescription);
        this.txc = txc;
        this.subdir = subdir;
        this.offset = offset;
        this.length = length;
        page = null;
        pointer = 0L;
    }

    @Override
    public void close() throws IOException {
        // Intentionally empty.
    }

    @Override
    public long getFilePointer() {
        return pointer;
    }

    @Override
    public long length() {
        return length;
    }

    @Override
    public byte readByte() throws IOException {
        if (pointer == length) {
            throw new EOFException("Attempt to read past end of file");
        }

        loadPageIfNull();
        byte result = page[FDBUtil.posToOffset(offset + pointer)];
        seek(this.pointer + 1);
        return result;
    }

    @Override
    public void readBytes(final byte[] b, final int offset, final int length) throws IOException {
        if (pointer + length > this.length) {
            throw new EOFException("Attempt to read past end of file");
        }

        int readOffset = offset;
        int readLength = length;
        while (readLength > 0) {
            loadPageIfNull();
            final int bytesToRead = Math.min(readLength,  page.length - FDBUtil.posToOffset(this.offset + this.pointer));
            System.arraycopy(page, FDBUtil.posToOffset(this.offset + this.pointer),
                    b, readOffset, bytesToRead);
            readOffset += bytesToRead;
            readLength -= bytesToRead;
            seek(pointer + bytesToRead);
        }
    }

    @Override
    public void seek(final long pos) throws IOException {
        if (pos > length) {
            throw new EOFException("Attempt to seek past end of file");
        }

        if (FDBUtil.posToPage(this.offset + pointer) != FDBUtil.posToPage(this.offset + pos)) {
            page = null;
        }
        pointer = pos;
    }

    @Override
    public IndexInput slice(final String sliceDescription, final long offset, final long length) throws IOException {
        if (offset < 0 || length < 0 || offset + length > this.length()) {
            throw new IllegalArgumentException("slice() " + sliceDescription + " out of bounds: " + this.length);
        }
        return new FDBIndexInput(getFullSliceDescription(sliceDescription), txc, subdir, this.offset + offset, length);
    }

    private void loadPageIfNull() {
        if (page == null) {
            final byte[] key = currentPageKey();
            page = txc.run(txn -> {
                return txn.get(key).join();
            });
        }
    }

    private byte[] currentPageKey() {
        final long currentPage = FDBUtil.posToPage(offset + pointer);
        return subdir.pack(currentPage);
    }

}

package com.cloudant.fdblucene;

import java.io.IOException;
import java.util.Arrays;

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
        if (page == null) {
            final byte[] key = currentPageKey();
            page = txc.run(txn -> {
                return txn.get(key).join();
            });
        }
        byte result = page[FDBUtil.posToOffset(offset + pointer)];
        seek(this.pointer + 1);
        return result;
    }

    @Override
    public void readBytes(final byte[] b, final int offset, final int len) throws IOException {
        // TODO optimise :)
        for (int i = 0; i < len; i++) {
            b[offset + i] = readByte();
        }
    }

    @Override
    public void seek(final long pos) throws IOException {
        if (FDBUtil.posToPage(pointer) != FDBUtil.posToPage(pos)) {
            page = null;
        }
        pointer = pos;
    }

    @Override
    public IndexInput slice(final String sliceDescription, final long offset, final long length) throws IOException {
        return new FDBIndexInput(getFullSliceDescription(sliceDescription), txc, subdir, offset, length);
    }

    private byte[] currentPageKey() {
        final long currentPage = FDBUtil.posToPage(offset + pointer);
        return subdir.pack(currentPage);
    }

}

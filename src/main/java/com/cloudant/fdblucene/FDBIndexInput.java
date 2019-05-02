package com.cloudant.fdblucene;

import java.io.EOFException;
import java.io.IOException;

import org.apache.commons.jcs.JCS;
import org.apache.commons.jcs.access.GroupCacheAccess;
import org.apache.lucene.store.IndexInput;

import com.apple.foundationdb.TransactionContext;
import com.apple.foundationdb.subspace.Subspace;

/**
 * A concrete implementation of {@link IndexInput} that reads {@code pages} from
 * FoundationDB. These pages are cached using {@link JCS} using an ephemeral key
 * that is valid only until {@link FDBDirectory#close()} is called.
 */
public final class FDBIndexInput extends IndexInput {

    private final TransactionContext txc;
    private final Subspace subspace;
    private final String name;
    private final long offset;
    private final long length;
    private byte[] page;
    private long pointer;
    private final int pageSize;

    private final ReadVersionCache readVersionCache;
    private final GroupCacheAccess<Long, byte[]> pageCache;

    FDBIndexInput(final String resourceDescription, final TransactionContext txc, final Subspace subspace,
            final String name, final long offset, final long length, final int pageSize,
            final GroupCacheAccess<Long, byte[]> pageCache) {
        super(resourceDescription);
        this.txc = txc;
        this.subspace = subspace;
        this.name = name;
        this.offset = offset;
        this.length = length;
        page = null;
        pointer = 0L;
        this.pageSize = pageSize;
        this.readVersionCache = new ReadVersionCache();
        this.pageCache = pageCache;
    }

    @Override
    public void close() throws IOException {
        // empty.
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
        byte result = page[FDBUtil.posToOffset(offset + pointer, pageSize)];
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
            final int bytesToRead = Math.min(readLength,
                    page.length - FDBUtil.posToOffset(this.offset + this.pointer, this.pageSize));
            System.arraycopy(page, FDBUtil.posToOffset(this.offset + this.pointer, this.pageSize), b, readOffset, bytesToRead);
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

        if (FDBUtil.posToPage(this.offset + pointer, pageSize) != FDBUtil.posToPage(this.offset + pos, this.pageSize)) {
            page = null;
        }
        pointer = pos;
    }

    @Override
    public IndexInput slice(final String sliceDescription, final long offset, final long length) throws IOException {
        if (offset < 0 || length < 0 || offset + length > this.length()) {
            throw new IllegalArgumentException("slice() " + sliceDescription + " out of bounds: " + this.length);
        }
        return new FDBIndexInput(getFullSliceDescription(sliceDescription), txc, subspace, name, this.offset + offset,
                length, pageSize, pageCache);
    }

    private void loadPageIfNull() {
        if (page == null) {
            final long currentPage = currentPage();
            page = pageCache.getFromGroup(currentPage, name);
            if (page == null) {
                final byte[] key = pageKey(currentPage);
                page = txc.run(txn -> {
                    readVersionCache.setReadVersion(txn);
                    txn.options().setTransactionLoggingEnable(String.format("%s,in,loadPage,%d", name, offset + pointer));
                    return txn.get(key).join();
                });
                pageCache.putInGroup(currentPage, name, page);
            }
        }
    }

    private long currentPage() {
        return FDBUtil.posToPage(offset + pointer, pageSize);
    }

    private byte[] pageKey(final long pageNumber) {
        return subspace.pack(pageNumber);
    }

}

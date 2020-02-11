/*******************************************************************************
 * Copyright 2019 IBM Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *******************************************************************************/
package com.cloudant.fdblucene;

import java.io.EOFException;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.IndexInput;
import com.apple.foundationdb.TransactionContext;
import com.apple.foundationdb.subspace.Subspace;

/**
 * A concrete implementation of {@link IndexInput} that reads {@code pages} from FoundationDB.
 */
public class FDBIndexInput extends BufferedIndexInput {

  private final TransactionContext txc;
  private final Subspace subspace;
  private final String name;
  private final long off;
  private final long end;
  private final int pageSize;
  private ReadVersionCache readVersionCache;

  public FDBIndexInput(final String resourceDescription, final TransactionContext txc,
      final Subspace subspace, final String name, final long off, final long length,
      final int pageSize) {
    super(resourceDescription, pageSize);
    this.txc = txc;
    this.subspace = subspace;
    this.name = name;
    this.off = off;
    this.end = off + length;
    this.pageSize = pageSize;
    this.readVersionCache = new ReadVersionCache();
  }

  @Override
  protected void readInternal(final byte[] b, final int offset, final int len) throws IOException {
    long pos = getFilePointer() + off;

    if (pos + len > end) {
      throw new EOFException("read past EOF: " + this);
    }

    int readLength = len;
    int copied = 0;
    long lastPageNumber = -1L;
    byte[] page = null;
    while (readLength > 0) {
      final long pageNumber = FDBUtil.posToPage(pos, pageSize);
      if (lastPageNumber != pageNumber) {
        page = loadPage(pageNumber);
        lastPageNumber = pageNumber;
      }
      final int toCopy = Math.min(page.length - FDBUtil.posToOffset(pos, pageSize), readLength);
      System.arraycopy(page, FDBUtil.posToOffset(pos, pageSize), b, offset + copied, toCopy);
      pos += toCopy;
      copied += toCopy;
      readLength -= toCopy;
    }
    assert readLength == 0;
  }

  @Override
  protected void seekInternal(final long pos) throws IOException {
    if (pos > length()) {
      throw new EOFException("read past EOF: pos=" + pos + " vs length=" + length() + ": " + this);
    }
  }

  @Override
  public IndexInput slice(final String sliceDescription, final long offset, final long length)
      throws IOException {
    if (offset < 0 || length < 0 || offset + length > this.length()) {
      throw new IllegalArgumentException("slice() " + sliceDescription + " out of bounds: offset="
          + offset + ",length=" + length + ",fileLength=" + this.length() + ": " + this);
    }
    return new FDBIndexInput(getFullSliceDescription(sliceDescription), txc, subspace, name,
        off + offset, length, pageSize);
  }

  @Override
  public FDBIndexInput clone() {
    final FDBIndexInput result = (FDBIndexInput) super.clone();
    // Clone ReadVersionCache instance as it is not thread-safe.
    result.readVersionCache = (ReadVersionCache) this.readVersionCache.clone();
    return result;
  }

  @Override
  public void close() throws IOException {
    // empty.
  }

  @Override
  public long length() {
    return end - off;
  }

  private byte[] loadPage(final long pageNumber) throws IOException {
    final byte[] key = pageKey(pageNumber);
    final byte[] result = txc.run(txn -> {
      readVersionCache.setReadVersion(txn);
      Utils.trace(txn, "%s,in,loadPage,%d", name, pageNumber);
      return txn.get(key).join();
    });
    if (result == null) {
      throw new EOFException("Read past end of file");
    }
    return result;
  }

  private byte[] pageKey(final long pageNumber) {
    return subspace.pack(pageNumber);
  }

}

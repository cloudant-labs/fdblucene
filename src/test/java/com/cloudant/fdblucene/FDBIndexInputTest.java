package com.cloudant.fdblucene;

import static org.junit.Assert.assertEquals;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Random;
import org.apache.lucene.store.IndexInput;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class FDBIndexInputTest extends BaseFDBTest {

    private static abstract class LazyCollection<T> implements Collection<T> {

        private final int size;

        public LazyCollection(final int size) {
            this.size = size;
        }

        protected abstract T create();

        @Override
        public int size() {
            return size;
        }

        @Override
        public boolean isEmpty() {
            return false;
        }

        @Override
        public boolean contains(Object o) {
            return false;
        }

        @Override
        public Iterator<T> iterator() {
            return new Iterator<T>() {

                private int i = 0;

                @Override
                public boolean hasNext() {
                    return i < size;
                }

                @Override
                public T next() {
                    i++;
                    return create();
                }

            };
        }

        @Override
        public Object[] toArray() {
            throw new UnsupportedOperationException("toArray not supported.");
        }

        @Override
        public <T> T[] toArray(T[] a) {
            throw new UnsupportedOperationException("toArray not supported.");
        }

        @Override
        public boolean add(T e) {
            throw new UnsupportedOperationException("add not supported.");
        }

        @Override
        public boolean remove(Object o) {
            throw new UnsupportedOperationException("remove not supported.");
        }

        @Override
        public boolean containsAll(Collection<?> c) {
            throw new UnsupportedOperationException("containsAll not supported.");
        }

        @Override
        public boolean addAll(Collection<? extends T> c) {
            throw new UnsupportedOperationException("addAll not supported.");
        }

        @Override
        public boolean removeAll(Collection<?> c) {
            throw new UnsupportedOperationException("removeAll not supported.");
        }

        @Override
        public boolean retainAll(Collection<?> c) {
            throw new UnsupportedOperationException("retainAll not supported.");
        }

        @Override
        public void clear() {
            throw new UnsupportedOperationException("clear not supported.");
        }

    }

    @Parameters
    public static Collection<Object[]> data() {
        final Random random = new Random();
        final Collection<Object[]> result = new LazyCollection<Object[]>(1000) {

            @Override
            protected Object[] create() {
                final int pageSize = 10 + random.nextInt(5000);
                final int pageCount = 2 + random.nextInt(5);

                final int len = 1 + random.nextInt((pageSize * pageCount) - 1);
                final int off = random.nextInt((pageSize * pageCount) - len);
                return new Object[] { off, len, pageCount, pageSize };
            }

        };

        return result;
    }

    private final long off;
    private final long length;
    private final int pageCount;
    private final int pageSize;

    public FDBIndexInputTest(final long off, final long length, final int pageCount, final int pageSize) {
        this.off = off;
        this.length = length;
        this.pageCount = pageCount;
        this.pageSize = pageSize;
    }

    @Before
    public void setup() throws Exception {
        super.setup();

        final byte[] value = new byte[pageSize];
        for (int i = 0; i < pageCount; i++) {
            final byte[] key = subspace.pack(i);
            Arrays.fill(value, (byte) (i % 255));
            DB.run(txn -> {
                txn.set(key, value);
                return null;
            });
        }
    }

    @Test
    public void testReadByte() throws Exception {
        try (final IndexInput in = new FDBIndexInput("foo", DB, subspace, "BAR", off, length, null, pageSize)) {
            final byte expectedValue = (byte) (FDBUtil.posToPage(off, pageSize) % 255);
            final String msg = String.format("off=%d, length=%d, pageSize=%d", off, length, pageSize);
            try {
                assertEquals(msg, expectedValue, in.readByte());
            } catch (IOException e) {
                System.err.println(msg);
                throw e;
            }
        }
    }

}

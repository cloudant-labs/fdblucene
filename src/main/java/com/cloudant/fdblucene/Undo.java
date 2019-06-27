package com.cloudant.fdblucene;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;

/**
 * Record transaction steps for later replay.
 */
public final class Undo {

    private static final Map<Integer, MutationType> BY_CODE;

    static {
        final Map<Integer, MutationType> map = new HashMap<Integer, MutationType>();
        for (final MutationType type : MutationType.values()) {
            map.put(type.code(), type);
        }
        BY_CODE = Collections.unmodifiableMap(map);
    }

    private static final int CLEAR_KEY = 0;
    private static final int CLEAR_RANGE = 1;
    private static final int MUTATE = 2;

    private static final int MAX_VALUE_LENGTH_BYTES = 4000;

    private final List<Tuple> actions = new ArrayList<Tuple>();

    public void clear(final byte[] key) {
        actions.add(Tuple.from(CLEAR_KEY, key));
    }

    public void clear(final byte[] beginKey, final byte[] endKey) {
        actions.add(Tuple.from(CLEAR_RANGE, beginKey, endKey));
    }

    public void clear(final Range range) {
        actions.add(Tuple.from(CLEAR_RANGE, range.begin, range.end));
    }

    public void mutate(final MutationType optype, final byte[] key, final byte[] param) {
        actions.add(Tuple.from(MUTATE, optype.code(), key, param));
    }

    public void run(final Transaction txn) {
        for (Tuple action : actions) {
            final int type = (int) action.getLong(0);
            switch (type) {
            case CLEAR_KEY:
                txn.clear(action.getBytes(1));
                break;
            case CLEAR_RANGE:
                txn.clear(action.getBytes(1), action.getBytes(2));
                break;
            case MUTATE:
                int code = (int) action.getLong(1);
                txn.mutate(BY_CODE.get(code), action.getBytes(2), action.getBytes(3));
                break;
            default:
                assert false;
                break;
            }
        }
    }

    public void save(final Transaction txn, final Subspace subspace) {
        // Marshal
        final byte[] marshalled;
        try (final ByteArrayOutputStream bos = new ByteArrayOutputStream();
                final DeflaterOutputStream fos = new DeflaterOutputStream(bos);
                final DataOutputStream dos = new DataOutputStream(fos)) {
            dos.writeByte(1);
            dos.writeShort(actions.size());
            for (final Tuple action : actions) {
                final byte[] packed = action.pack();
                dos.writeShort(packed.length);
                dos.write(packed);
            }
            fos.finish();
            marshalled = bos.toByteArray();
        } catch (final IOException e) {
            throw new Error("ByteArrayOutputStream threw IOException", e);
        }

        // Persist
        txn.clear(subspace.range());
        for (int i = 0; i < marshalled.length; i += MAX_VALUE_LENGTH_BYTES) {
            final int len = Math.min(MAX_VALUE_LENGTH_BYTES, marshalled.length - i);
            final byte[] chunk = Arrays.copyOfRange(marshalled, i, len);
            txn.set(subspace.pack(i), chunk);
        }
    }

    public void load(final Transaction txn, final Subspace subspace) {
        actions.clear();
        try (final ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            // Fetch
            txn.getRange(subspace.range()).forEach(kv -> {
                try {
                    bos.write(kv.getValue());
                } catch (final IOException e) {
                    assert false;
                }
            });

            // Unmarshal
            final ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
            final InflaterInputStream iis = new InflaterInputStream(bis);
            final DataInputStream dis = new DataInputStream(iis);
            final int version = dis.readByte();
            if (version != 1) {
                throw new IllegalArgumentException();
            }
            final int count = dis.readShort();
            for (int i = 0; i < count; i++) {
                final byte[] bytes = new byte[dis.readShort()];
                dis.readFully(bytes);
                final Tuple action = Tuple.fromBytes(bytes);
                actions.add(action);
            }
        } catch (final IOException e) {
            throw new Error("ByteArrayOutputStream threw IOException", e);
        }
    }

    public String toString() {
        return actions.toString();
    }
}

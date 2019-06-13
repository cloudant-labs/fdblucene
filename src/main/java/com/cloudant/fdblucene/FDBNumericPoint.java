package com.cloudant.fdblucene;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;

public class FDBNumericPoint extends Field {

    public final static FieldType TYPE;
    static {
        TYPE = new FieldType();
        TYPE.setIndexOptions(IndexOptions.DOCS);
        TYPE.setTokenized(false);
        TYPE.setOmitNorms(true);
        TYPE.freeze();
    }

    public FDBNumericPoint(final String name, final Number value) {
        super(name, numberToBytes(value), TYPE);
    }

    public static Query newExactQuery(final String field, final Number value) {
        return new TermQuery(new Term(field, numberToRef(value)));
    }

    public static Query newRangeQuery(final String field, final Number lowerValue, final Number upperValue) {
        return new TermRangeQuery(field, numberToRef(lowerValue), numberToRef(upperValue), true, true);
    }

    static BytesRef numberToRef(final Number number) {
        return new BytesRef(numberToBytes(number));
    }

    static byte[] numberToBytes(final Number number) {
        if (number instanceof Double) {
            final long asLong = NumericUtils.doubleToSortableLong((Double) number);
            return numberToBytes(asLong);
        } else if (number instanceof Float) {
            final int asInt = NumericUtils.floatToSortableInt((Float) number);
            return numberToBytes(asInt);
        } else if (number instanceof Long) {
            final byte[] result = new byte[8];
            NumericUtils.longToSortableBytes((Long) number, result, 0);
            return result;
        } else if (number instanceof Integer) {
            final byte[] result = new byte[4];
            NumericUtils.intToSortableBytes((Integer) number, result, 0);
            return result;
        }
        throw new IllegalArgumentException(number + " not supported.");
    }

}
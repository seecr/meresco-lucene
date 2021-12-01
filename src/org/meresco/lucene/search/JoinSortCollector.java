/* begin license *
 *
 * "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
 *
 * Copyright (C) 2015-2016 Koninklijke Bibliotheek (KB) http://www.kb.nl
 * Copyright (C) 2015-2019 Seecr (Seek You Too B.V.) https://seecr.nl
 * Copyright (C) 2016 Stichting Kennisnet http://www.kennisnet.nl
 *
 * This file is part of "Meresco Lucene"
 *
 * "Meresco Lucene" is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * "Meresco Lucene" is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with "Meresco Lucene"; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
 *
 * end license */

package org.meresco.lucene.search;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.List;

import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.comparators.IntComparator;
import org.apache.lucene.search.comparators.DoubleComparator;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortField.Type;
import org.apache.lucene.util.BytesRef;
import org.meresco.lucene.search.join.KeyValuesCache;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.FilterNumericDocValues;


public class JoinSortCollector extends SimpleCollector {
    protected String resultKeyName;
    private String otherKeyName;
    private int[] keys;
    private int docBase;
    private IndexReaderContext topLevelReaderContext;
    private static int docIdsByKeyInitialSize = 0;
    protected int[] docIdsByKey = new int[docIdsByKeyInitialSize];

    public JoinSortCollector(String resultKeyName, String otherKeyName) {
        this.resultKeyName = resultKeyName;
        this.otherKeyName = otherKeyName;
    }

    public FieldComparator<?> getComparator(String field, Type type, boolean reverse, final int numHits, final int sortPos, Object missingValue) {
        switch(type) {
            case STRING:
                return new JoinTermValComparator(numHits, this.resultKeyName, field, SortField.STRING_LAST, this);
            case INT:
                return new JoinIntComparator(numHits, this.resultKeyName, field, (Integer) missingValue, this, reverse, sortPos);
            case DOUBLE:
                return new JoinDoubleComparator(numHits, this.resultKeyName, field, (Double) missingValue, this, reverse, sortPos);
            default:
                throw new IllegalStateException("Illegal join sort type: " + type);
        }
    }

    @Override
    public void collect(int doc) throws IOException {
        if (this.keys == null)
            return;
        int key = this.keys[doc];
        if (key == 0)
            return;
        if (key >= this.docIdsByKey.length)
            resizeDocIdsByKey((int) ((key + 1) * 1.25));
        this.docIdsByKey[key] = doc + docBase + 1;  // increment to distinguish docId==0 from key not present
    }

    void resizeDocIdsByKey(int newSize) {
        if (newSize <= docIdsByKey.length) {
            return;
        }
        docIdsByKeyInitialSize  = newSize;
        int[] dest = new int[newSize];
        System.arraycopy(docIdsByKey, 0, dest, 0, docIdsByKey.length);
        this.docIdsByKey = dest;
    }

    @Override
    public ScoreMode scoreMode() {
        return ScoreMode.COMPLETE_NO_SCORES;
    }

    @Override
    public void doSetNextReader(LeafReaderContext context) throws IOException {
        if (this.topLevelReaderContext == null) {
            this.topLevelReaderContext = ReaderUtil.getTopLevelContext(context);
        }
        keys = KeyValuesCache.get(context, this.otherKeyName);
        docBase = context.docBase;
    }

    public long getNumericSortValue(int key, String sortField) throws IOException {
        if (key < this.docIdsByKey.length) {
            int otherDoc = this.docIdsByKey[key] -1;
            LeafReaderContext context = this.contextForDocId(otherDoc);
            NumericDocValues sort_values = DocValues.getNumeric(context.reader(), sortField);
            if (sort_values.advanceExact(otherDoc - context.docBase))
                return sort_values.longValue();
        }
        return -1;
    }

    public BytesRef getBinarySortValue(int key, String sortField) throws IOException {
        if (key < this.docIdsByKey.length) {
            int otherDoc = this.docIdsByKey[key] -1;
            LeafReaderContext context = this.contextForDocId(otherDoc);
            SortedDocValues sort_values = DocValues.getSorted(context.reader(), sortField);
            if (sort_values.advanceExact(otherDoc - context.docBase))
                return sort_values.binaryValue();
        }
        return null;
    }

    private LeafReaderContext contextForDocId(int docId) {
        List<LeafReaderContext> leaves = this.topLevelReaderContext.leaves();
        return leaves.get(ReaderUtil.subIndex(docId, leaves));
    }

}


class JoinTermValComparator extends FieldComparator.TermValComparator {
    /*
     * See JoinIntComparator for principle
     */
    JoinSortCollector collector;
    String otherSortField;

    public JoinTermValComparator(int numHits, String resultKeyName, String otherSortField, Object missingValue, JoinSortCollector collector) {
        super(numHits, resultKeyName, missingValue == SortField.STRING_LAST);
        this.collector = collector;
        this.otherSortField = otherSortField;
    }

    @Override
    protected BinaryDocValues getBinaryDocValues(LeafReaderContext context, String field) throws IOException {
        if (context == null)
            return DocValues.emptyBinary();
        NumericDocValues resultKeys = DocValues.getNumeric(context.reader(), field);
        return new BinaryDocValues() {
            @Override
            public boolean advanceExact(int resultDoc) throws IOException {
                return resultKeys.advanceExact(resultDoc);
            }
            @Override
            public BytesRef binaryValue() throws IOException {
                int key = (int) resultKeys.longValue();
                return collector.getBinarySortValue(key, otherSortField);
            }
            @Override
            public long cost() { throw new java.lang.UnsupportedOperationException(); }
            @Override
            public int advance(int target) { throw new java.lang.UnsupportedOperationException(); }
            @Override
            public int nextDoc() { throw new java.lang.UnsupportedOperationException(); }
            @Override
            public int docID() { throw new java.lang.UnsupportedOperationException(); }
        };
    }
}


class JoinIntComparator extends IntComparator {
    /*
     * Supports sorting of one core (resultCore) based on values in another core (otherCore).
     * The fields resultKeyName and otherKeyName  contain numeric keys which must match.
     * (The corresponding otherKeyName  must be passed to the JoinSortCollector.)
     * Sorting is performed on the numeric values found in sortField.
     *
     * This implementation lets IntComparator fetchs the keys for resultCore and maps them to
     * otherCore sort values in longValue(), using the data in the collector. That is only
     * one line of relavant code, the rest is clutter.
     */
    JoinSortCollector collector;
    String sortField;

    JoinIntComparator(int numHits, String resultKeyName, String sortField, Integer missingValue,
            JoinSortCollector collector, boolean reverse, int sortPos) {
        super(numHits, resultKeyName, missingValue == null ? 0 : missingValue, reverse, sortPos);
        this.collector = collector;
        this.sortField = sortField;
    }

    @Override
    public LeafFieldComparator getLeafComparator(LeafReaderContext resultContext) throws IOException {
        return new IntComparator.IntLeafComparator(resultContext) {
            @Override
            public NumericDocValues getNumericDocValues(LeafReaderContext resultContext, String resultKeyName) throws IOException {
                return new FilterNumericDocValues(super.getNumericDocValues(resultContext, resultKeyName)) {
                    @Override
                    public long longValue() throws IOException {
                        return collector.getNumericSortValue((int) super.longValue(), sortField);
                    }
                };
            }
        };
    }
}

class JoinDoubleComparator extends DoubleComparator {
    /*
     * See JoinIntComparator for how it works
     */
    JoinSortCollector collector;
    String sortField;

    JoinDoubleComparator(int numHits, String resultKeyName, String sortField, Double missingValue,
            JoinSortCollector collector, boolean reverse, int sortPos) {
        super(numHits, resultKeyName, missingValue == null ? 0 : missingValue, reverse, sortPos);
        this.collector = collector;
        this.sortField = sortField;
    }

    @Override
    public LeafFieldComparator getLeafComparator(LeafReaderContext resultContext) throws IOException {
        return new DoubleComparator.DoubleLeafComparator(resultContext) {
            @Override
            public NumericDocValues getNumericDocValues(LeafReaderContext resultContext, String resultKeyName) throws IOException {
                return new FilterNumericDocValues(super.getNumericDocValues(resultContext, resultKeyName)) {
                    @Override
                    public long longValue() throws IOException {
                        return collector.getNumericSortValue((int) super.longValue(), sortField);
                    }
                };
            }
        };
    }
}

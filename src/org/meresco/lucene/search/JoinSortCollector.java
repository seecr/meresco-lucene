/* begin license *
 *
 * "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
 *
 * Copyright (C) 2015 Koninklijke Bibliotheek (KB) http://www.kb.nl
 * Copyright (C) 2015 Seecr (Seek You Too B.V.) http://seecr.nl
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

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortField.Type;
import org.apache.lucene.util.BytesRef;
import org.meresco.lucene.search.join.KeyValuesCache;


interface JoinFieldComparator {
    public int compareBottom(int doc);
    public int compareTop(int doc);
    public void copy(int slot, int doc);
    void setOtherCoreContext(AtomicReaderContext context);
}


public class JoinSortCollector extends Collector {
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

    public SortField sortField(String field, Type type, boolean reverse) {
        return new JoinSortField(field, type, reverse, this);
    }

    public FieldComparator<?> getComparator(String field, Type type, boolean reverse, final int numHits, final int sortPos, Object missingValue) throws IOException {
        switch(type) {
            case STRING:
                return new JoinTermOrdValComparator(numHits, field, missingValue == SortField.STRING_LAST, this);
            case INT:
                return new JoinIntComparator(numHits, field, (Integer) missingValue, this);
            case DOUBLE:
                return new JoinDoubleComparator(numHits, field, (Double) missingValue, this);
            default:
                throw new IllegalStateException("Illegal join sort type: " + type);
        }
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {}

    @Override
    public void collect(int doc) throws IOException {
        int key = this.keys[doc];
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
    public void setNextReader(AtomicReaderContext context) throws IOException {
        if (this.topLevelReaderContext == null) {
            this.topLevelReaderContext = ReaderUtil.getTopLevelContext(context);
        }
        keys = KeyValuesCache.get(context, this.otherKeyName);
        docBase = context.docBase;
    }

    @Override
    public boolean acceptsDocsOutOfOrder() {
        return false;
    }

    int otherDocIdForKey(int key, JoinFieldComparator comparator) {
        if (key < this.docIdsByKey.length) {
            int otherDoc = this.docIdsByKey[key];
            if (otherDoc > 0) {
                otherDoc -= 1;
                AtomicReaderContext context = this.contextForDocId(otherDoc);
                comparator.setOtherCoreContext(context);
                return otherDoc - context.docBase;
            }
        }
        comparator.setOtherCoreContext(null);
        return -1;
    }

    private AtomicReaderContext contextForDocId(int docId) {
        List<AtomicReaderContext> leaves = this.topLevelReaderContext.leaves();
        return leaves.get(ReaderUtil.subIndex(docId, leaves));
    }
}


class JoinTermOrdValComparator extends FieldComparator.TermOrdValComparator implements JoinFieldComparator {
    final private JoinSortCollector collector;
    private int[] resultKeys;

    public JoinTermOrdValComparator(int numHits, String field, boolean reverse, JoinSortCollector collector) {
        super(numHits, field, reverse);
        this.collector = collector;
    }

    @Override
    protected SortedDocValues getSortedDocValues(AtomicReaderContext context, String field) throws IOException {
        if (context != null)
            return super.getSortedDocValues(context, field);
        return new SortedDocValues() {

            @Override
            public int getOrd(int docID) {
                return -1;
            }

            @Override
            public int lookupTerm(BytesRef key) {
                return -1;
            }

            @Override
            public BytesRef lookupOrd(int ord) {
                return null;
            }

            @Override
            public int getValueCount() {
                return 0;
            }
        };
    }

    @Override
    public JoinTermOrdValComparator setNextReader(AtomicReaderContext context) throws IOException {
        this.resultKeys = KeyValuesCache.get(context, this.collector.resultKeyName);
        return this;
    }

    @Override
    public int compareBottom(int doc) {
        return super.compareBottom(this.collector.otherDocIdForKey(resultKeys[doc], this));
    }

    @Override
    public int compareTop(int doc) {
        return super.compareTop(this.collector.otherDocIdForKey(resultKeys[doc], this));
    }

    @Override
    public void copy(int slot, int doc) {
        super.copy(slot, this.collector.otherDocIdForKey(resultKeys[doc], this));
    }

    public void setOtherCoreContext(AtomicReaderContext context) {
        try {
            super.setNextReader(context);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}


class JoinIntComparator extends FieldComparator.IntComparator implements JoinFieldComparator {
    private JoinSortCollector collector;
    private int[] resultKeys;
    private Integer topValue;
    private Integer bottomValue;
    private int[] values;

    public JoinIntComparator(int numHits, String field, Integer missingValue, JoinSortCollector collector) {
        super(numHits, field, null, missingValue == null ? 0 : missingValue);
        this.collector = collector;
        this.values = accessValuesFromSuper();
    }

    private int[] accessValuesFromSuper() {
        try {
            Field valuesField = FieldComparator.IntComparator.class.getDeclaredField("values");
            valuesField.setAccessible(true);
            return (int[]) valuesField.get(this);
        } catch (NoSuchFieldException | SecurityException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public JoinIntComparator setNextReader(AtomicReaderContext context) throws IOException {
        this.resultKeys = KeyValuesCache.get(context, this.collector.resultKeyName);
        return this;
    }

    public void setTopValue(Integer value) {
        this.topValue = value;
        super.setTopValue(value);
    }

    public void setBottom(final int bottom) {
        this.bottomValue = this.value(bottom);
        super.setBottom(bottom);
    }

    @Override
    public int compareBottom(int doc) {
        int otherDoc = this.collector.otherDocIdForKey(this.resultKeys[doc], this);
        if (otherDoc == -1)
            return Integer.compare(this.bottomValue, this.missingValue);
        return super.compareBottom(otherDoc);
    }

    @Override
    public int compareTop(int doc) {
        int otherDoc = this.collector.otherDocIdForKey(this.resultKeys[doc], this);
        if (otherDoc == -1)
            return Integer.compare(this.topValue, this.missingValue);
        return super.compareTop(otherDoc);
    }

    @Override
    public void copy(int slot, int doc) {
        int otherDoc = this.collector.otherDocIdForKey(this.resultKeys[doc], this);
        if (otherDoc == -1) {
            values[slot] = this.missingValue;
        }
        else {
            super.copy(slot, otherDoc);
        }
    }

    public void setOtherCoreContext(AtomicReaderContext context) {
        if (context == null) {
            return;
        }
        try {
            super.setNextReader(context);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}


class JoinDoubleComparator extends FieldComparator.DoubleComparator implements JoinFieldComparator {
    private JoinSortCollector collector;
    private int[] resultKeys;
    private Double topValue;
    private Double bottomValue;
    private double[] values;

    public JoinDoubleComparator(int numHits, String field, Double missingValue, JoinSortCollector collector) {
        super(numHits, field, null, missingValue == null ? 0 : missingValue);
        this.collector = collector;
        this.values = accessValuesFromSuper();
    }

    private double[] accessValuesFromSuper() {
        try {
            Field valuesField = FieldComparator.DoubleComparator.class.getDeclaredField("values");
            valuesField.setAccessible(true);
            return (double[]) valuesField.get(this);
        } catch (NoSuchFieldException | SecurityException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public JoinDoubleComparator setNextReader(AtomicReaderContext context) throws IOException {
        this.resultKeys = KeyValuesCache.get(context, this.collector.resultKeyName);
        return this;
    }

    public void setTopValue(Double value) {
        this.topValue = value;
        super.setTopValue(value);
    }

    public void setBottom(final int bottom) {
        this.bottomValue = this.value(bottom);
        super.setBottom(bottom);
    }

    @Override
    public int compareBottom(int doc) {
        int otherDoc = this.collector.otherDocIdForKey(this.resultKeys[doc], this);
        if (otherDoc == -1)
            return Double.compare(this.bottomValue, this.missingValue);
        return super.compareBottom(otherDoc);
    }

    @Override
    public int compareTop(int doc) {
        int otherDoc = this.collector.otherDocIdForKey(this.resultKeys[doc], this);
        if (otherDoc == -1)
            return Double.compare(this.topValue, this.missingValue);
        return super.compareTop(otherDoc);
    }

    @Override
    public void copy(int slot, int doc) {
        int otherDoc = this.collector.otherDocIdForKey(this.resultKeys[doc], this);
        if (otherDoc == -1) {
            values[slot] = this.missingValue;
        }
        else {
            super.copy(slot, otherDoc);
        }
    }

    public void setOtherCoreContext(AtomicReaderContext context) {
        if (context == null) {
            return;
        }
        try {
            super.setNextReader(context);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

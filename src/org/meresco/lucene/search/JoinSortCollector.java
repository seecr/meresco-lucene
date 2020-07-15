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
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortField.Type;
import org.apache.lucene.util.BytesRef;
import org.meresco.lucene.search.join.KeyValuesCache;


interface JoinFieldComparator {
    public int compareBottom(int doc);
    public int compareTop(int doc);
    public void copy(int slot, int doc);
    void setOtherCoreContext(LeafReaderContext context);
}


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

    int otherDocIdForKey(int key, JoinFieldComparator comparator) {
        if (key < this.docIdsByKey.length) {
            int otherDoc = this.docIdsByKey[key];
            if (otherDoc > 0) {
                otherDoc -= 1;
                LeafReaderContext context = this.contextForDocId(otherDoc);
                comparator.setOtherCoreContext(context);
                return otherDoc - context.docBase;
            }
        }
        comparator.setOtherCoreContext(null);
        return -1;
    }

    private LeafReaderContext contextForDocId(int docId) {
        List<LeafReaderContext> leaves = this.topLevelReaderContext.leaves();
        return leaves.get(ReaderUtil.subIndex(docId, leaves));
    }

}


class JoinTermOrdValComparator extends FieldComparator.TermOrdValComparator implements JoinFieldComparator, LeafFieldComparator {
    final private JoinSortCollector collector;
    private int[] resultKeys;

    public JoinTermOrdValComparator(int numHits, String field, boolean reverse, JoinSortCollector collector) {
        super(numHits, field, reverse);
        this.collector = collector;
    }

    @Override
    protected SortedDocValues getSortedDocValues(LeafReaderContext context, String field) throws IOException {
        if (context != null)
            return super.getSortedDocValues(context, field);
        return new SortedDocValues() {

            @Override
            public int ordValue() throws IOException {
                return -1;
            }

            @Override
            public BytesRef lookupOrd(int ord) throws IOException {
                return null;
            }

            @Override
            public int getValueCount() {
                return 0;
            }

            @Override
            public boolean advanceExact(int target) throws IOException {
                return false;
            }

            @Override
            public int docID() {
                return 0;
            }

            @Override
            public int nextDoc() throws IOException {
                return 0;
            }

            @Override
            public int advance(int target) throws IOException {
                return 0;
            }

            @Override
            public long cost() {
                return 0;
            }
        };
    }

    @Override
    public JoinTermOrdValComparator getLeafComparator(LeafReaderContext context) throws IOException {
        this.resultKeys = KeyValuesCache.get(context, this.collector.resultKeyName);
        return this;
    }

    @Override
    public int compareBottom(int doc) {
        try {
            return super.compareBottom(this.collector.otherDocIdForKey(this.resultKeys != null ? this.resultKeys[doc] : 0, this));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int compareTop(int doc) {
        try {
            return super.compareTop(this.collector.otherDocIdForKey(this.resultKeys != null ? this.resultKeys[doc] : 0, this));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void copy(int slot, int doc) {
        try {
            super.copy(slot, this.collector.otherDocIdForKey(this.resultKeys != null ? this.resultKeys[doc] : 0, this));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
	public void setOtherCoreContext(LeafReaderContext context) {
        try {
            super.getLeafComparator(context);
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
    private static Field valuesField;

    {{
		try {
			valuesField = FieldComparator.IntComparator.class.getDeclaredField("values");
	        valuesField.setAccessible(true);
		} catch (NoSuchFieldException | SecurityException e) {
            throw new RuntimeException(e);
		}
    }}

    public JoinIntComparator(int numHits, String field, Integer missingValue, JoinSortCollector collector) {
        super(numHits, field, missingValue == null ? 0 : missingValue);
        this.collector = collector;
        try {
            this.values = (int[]) valuesField.get(this);
        } catch (SecurityException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void doSetNextReader(LeafReaderContext context) throws IOException {
        this.resultKeys = KeyValuesCache.get(context, this.collector.resultKeyName);
        super.doSetNextReader(context);
    }

    @Override
	public void setTopValue(Integer value) {
        this.topValue = value;
        super.setTopValue(value);
    }

    @Override
	public void setBottom(final int bottom) {
        this.bottomValue = this.value(bottom);
        super.setBottom(bottom);
    }

    @Override
    public int compareBottom(int doc) {
        int otherDoc = this.collector.otherDocIdForKey(this.resultKeys != null ? this.resultKeys[doc] : 0, this);
        if (otherDoc == -1)
            return Integer.compare(this.bottomValue, this.missingValue);
        try {
            return super.compareBottom(otherDoc);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int compareTop(int doc) {
        int otherDoc = this.collector.otherDocIdForKey(this.resultKeys != null ? this.resultKeys[doc] : 0, this);
        if (otherDoc == -1)
            return Integer.compare(this.topValue, this.missingValue);
        try {
            return super.compareTop(otherDoc);
        } catch (IOException e) {
           throw new RuntimeException(e);
        }
    }

    @Override
    public void copy(int slot, int doc) {
        int otherDoc = this.collector.otherDocIdForKey(this.resultKeys != null ? this.resultKeys[doc] : 0, this);
        if (otherDoc == -1) {
            values[slot] = this.missingValue;
        }
        else {
            try {
                super.copy(slot, otherDoc);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
	public void setOtherCoreContext(LeafReaderContext context) {
        if (context == null) {
            return;
        }
        try {
            super.doSetNextReader(context);
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
    private static Field valuesField;

    {{
		try {
			valuesField = FieldComparator.DoubleComparator.class.getDeclaredField("values");
	        valuesField.setAccessible(true);
		} catch (NoSuchFieldException | SecurityException e) {
            throw new RuntimeException(e);
		}
    }}

    public JoinDoubleComparator(int numHits, String field, Double missingValue, JoinSortCollector collector) {
        super(numHits, field, missingValue == null ? 0 : missingValue);
        this.collector = collector;
        try {
            this.values = (double[]) valuesField.get(this);
        } catch (SecurityException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void doSetNextReader(LeafReaderContext context) throws IOException {
        this.resultKeys = KeyValuesCache.get(context, this.collector.resultKeyName);
        super.doSetNextReader(context);
    }

    @Override
	public void setTopValue(Double value) {
        this.topValue = value;
        super.setTopValue(value);
    }

    @Override
	public void setBottom(final int bottom) {
        this.bottomValue = this.value(bottom);
        super.setBottom(bottom);
    }

    @Override
    public int compareBottom(int doc) {
        int otherDoc = this.collector.otherDocIdForKey(this.resultKeys != null ? this.resultKeys[doc] : 0, this);
        if (otherDoc == -1)
            return Double.compare(this.bottomValue, this.missingValue);
        try {
            return super.compareBottom(otherDoc);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int compareTop(int doc) {
        int otherDoc = this.collector.otherDocIdForKey(this.resultKeys != null ? this.resultKeys[doc] : 0, this);
        if (otherDoc == -1)
            return Double.compare(this.topValue, this.missingValue);
        try {
            return super.compareTop(otherDoc);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void copy(int slot, int doc) {
        int otherDoc = this.collector.otherDocIdForKey(this.resultKeys != null ? this.resultKeys[doc] : 0, this);
        if (otherDoc == -1) {
            values[slot] = this.missingValue;
        }
        else {
            try {
                super.copy(slot, otherDoc);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
	public void setOtherCoreContext(LeafReaderContext context) {
        if (context == null) {
            return;
        }
        try {
            super.doSetNextReader(context);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

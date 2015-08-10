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
import java.util.List;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortField.Type;
import org.apache.lucene.util.BytesRef;
import org.meresco.lucene.search.join.KeyValuesCache;


interface JoinFieldComparator {
    int getKey(int docId);
    void superSetNextReader(AtomicReaderContext context);
}


public class JoinSortCollector extends Collector {
    private String resultKeyname;
    private String otherKeyname;
    protected int[] docIdsByKey;
    private int[] keys;
    private int docBase;
    private IndexReaderContext topLevelReaderContext;
    
    public JoinSortCollector(String resultKeyname, String otherKeyname) {
        this.resultKeyname = resultKeyname;
        this.otherKeyname = otherKeyname;
    }

    public SortField sortField(String field, Type type, boolean reverse) {
        return new JoinSortField(field, type, reverse, this);
    }

    public FieldComparator<?> getComparator(String field, Type type, boolean reverse, final int numHits, final int sortPos, Object missingValue) throws IOException {
        switch(type) {
            case STRING:
                return new JoinTermOrdValComparator(numHits, field, missingValue == SortField.STRING_LAST, this.resultKeyname, this);
            case INT:
                return new JoinIntComparator(numHits, field, (Integer) missingValue, this.resultKeyname, this);
            default:
                throw new IllegalStateException("Illegal join sort type: " + type);
        } 
    }
    
    @Override
    public void setScorer(Scorer scorer) throws IOException {}

    @Override
    public void collect(int doc) throws IOException {
        int key = this.keys[doc];
        if (key >= docIdsByKey.length)
            resizeKeys((int) ((key + 1) * 1.25));
        docIdsByKey[key] = doc + docBase;
    }
    
    void resizeKeys(int newSize) {
        if (newSize <= docIdsByKey.length) {
            return;
        }
        int[] dest = new int[newSize];
        System.arraycopy(docIdsByKey, 0, dest, 0, docIdsByKey.length);
        this.docIdsByKey = dest;
    }

    @Override
    public void setNextReader(AtomicReaderContext context) throws IOException {
        if (this.topLevelReaderContext == null) {
            this.topLevelReaderContext = ReaderUtil.getTopLevelContext(context);
            this.docIdsByKey = new int[this.topLevelReaderContext.reader().maxDoc()];
        }
        keys = KeyValuesCache.get(context, this.otherKeyname);
        docBase = context.docBase;
    }
    
    public AtomicReaderContext contextForDocId(int docId) {
        List<AtomicReaderContext> leaves = this.topLevelReaderContext.leaves();
        return leaves.get(ReaderUtil.subIndex(docId, leaves));
    }
    
    @Override
    public boolean acceptsDocsOutOfOrder() {
        return false;
    }
    
    int otherDocIdForDocId(int doc, JoinFieldComparator comparator) {
        int key = comparator.getKey(doc);
        if (key < this.docIdsByKey.length) { 
            int otherDoc = this.docIdsByKey[key];  // TODO: deal with docId==0
            if (otherDoc > 0) {
                AtomicReaderContext context = this.contextForDocId(otherDoc);
                comparator.superSetNextReader(context);
                return otherDoc - context.docBase;
            }
        }
        comparator.superSetNextReader(null);
        return -1;
    }
}


class JoinTermOrdValComparator extends FieldComparator.TermOrdValComparator implements JoinFieldComparator {
    private int[] keys;
    final private String keyname;
    final private JoinSortCollector collector;
    
    public JoinTermOrdValComparator(int numHits, String field, boolean reverse, String keyname, JoinSortCollector collector)
    {
        super(numHits, field, reverse);
        this.keyname = keyname;
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
    public FieldComparator<BytesRef> setNextReader(AtomicReaderContext context) throws IOException {
        this.keys = KeyValuesCache.get(context, keyname);
        return this;
    }
    
    @Override
    public int compareBottom(int doc) {
        return super.compareBottom(this.collector.otherDocIdForDocId(doc, this));
    }

    @Override
    public int compareTop(int doc) {
        return super.compareTop(this.collector.otherDocIdForDocId(doc, this));
    }

    @Override
    public void copy(int slot, int doc) {
        super.copy(slot, this.collector.otherDocIdForDocId(doc, this));
    }
    
    public void superSetNextReader(AtomicReaderContext context) {
        try {
            super.setNextReader(context);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }   
    }
    
    @Override
    public int getKey(int docId) {
        return this.keys[docId];
    }
}



class JoinIntComparator extends FieldComparator.IntComparator implements JoinFieldComparator {
    private int[] keys;
    private String keyname;
    private JoinSortCollector collector;
    
    public JoinIntComparator(int numHits, String field, Integer missingValue, String keyname, JoinSortCollector collector)
    {
        super(numHits, field, null, missingValue);
        this.keyname = keyname;
        this.collector = collector;
    }
    
    @Override
    protected FieldCache.Ints getIntValues(AtomicReaderContext context, String field) throws IOException {
        if (context != null)
            return super.getIntValues(context, field);
        
        return new FieldCache.Ints() {
            @Override
            public int get(int docID) {
                return 0;
            }
        };
    }
    
    @Override
    public FieldComparator<Integer> setNextReader(AtomicReaderContext context) throws IOException {
        this.keys = KeyValuesCache.get(context, keyname);
        return this;
    }
    
    @Override
    public int compareBottom(int doc) {
        return super.compareBottom(this.collector.otherDocIdForDocId(doc, this));
    }

    @Override
    public int compareTop(int doc) {
        return super.compareTop(this.collector.otherDocIdForDocId(doc, this));
    }

    @Override
    public void copy(int slot, int doc) {
        super.copy(slot, this.collector.otherDocIdForDocId(doc, this));
    }
    
    public void superSetNextReader(AtomicReaderContext context) {
        try {
            super.setNextReader(context);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }   
    }
    
    @Override
    public int getKey(int docId) {
        return this.keys[docId];
    }
}
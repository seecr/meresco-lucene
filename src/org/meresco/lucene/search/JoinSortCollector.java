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
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortField.Type;
import org.apache.lucene.util.BytesRef;
import org.meresco.lucene.search.join.KeyValuesCache;

public class JoinSortCollector extends Collector {

    public FieldComparator<BytesRef> comparator;
    private String keyname;
    private Map<AtomicReaderContext, int[]> contextToKeys = new HashMap<AtomicReaderContext, int[]>();

    public JoinSortCollector(String keyname) {
        this.keyname = keyname;
    }

    public void setComparator(String field, Type type, boolean reverse, final int numHits, final int sortPos, Object missingValue) throws IOException {
        System.out.println("Comparator for field: " + field);
        // this.comparator = (FieldComparator<BytesRef>) new SortField(field, type, reverse).getComparator(numHits, sortPos);
        this.comparator = new FieldComparator.TermOrdValComparator(numHits, field, missingValue == SortField.STRING_LAST) {
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
        };
    }

    public int setNextReaderForKey(int key) throws IOException {
        for (Entry<AtomicReaderContext, int[]> set : contextToKeys.entrySet()) {
            AtomicReaderContext context = set.getKey();
            int[] docValues = set.getValue();
            for (int i = 0; i < docValues.length; i++) {
                if (docValues[i] == key) {
                    this.comparator.setNextReader(context);
                    return i;
                }
            }
        }
        this.comparator.setNextReader(null);
        return -1;
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {

    }

    @Override
    public void collect(int doc) throws IOException {

    }

    @Override
    public void setNextReader(AtomicReaderContext context) throws IOException {
        this.contextToKeys.put(context, KeyValuesCache.get(context, this.keyname));
    }

    @Override
    public boolean acceptsDocsOutOfOrder() {
        return false;
    }
}

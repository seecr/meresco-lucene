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

import org.apache.commons.math3.analysis.function.Max;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BytesRef;
import org.meresco.lucene.search.join.KeyValuesCache;

public class JoinSortField extends SortField {

    private JoinSortCollector joinSortCollector;
    private String keyname;

    public JoinSortField(String field, Type type, boolean reverse, JoinSortCollector joinSortCollector, String keyname) {
        super(field, type, reverse);
        this.joinSortCollector = joinSortCollector;
        this.keyname = keyname;
    }

    public FieldComparator<?> getComparator(final int numHits, final int sortPos) throws IOException {
        this.joinSortCollector.setComparator(this.getField(), this.getType(), this.getReverse(), numHits, sortPos, this.missingValue);
        return new JoinComparator(this.keyname, this.joinSortCollector, this.missingValue);
    }
}

class JoinComparator extends FieldComparator<BytesRef> {

    private String keyname;
    private int[] keyValuesArray;
    private JoinSortCollector joinSortCollector;

    public JoinComparator(String keyname, JoinSortCollector joinSortCollector, Object missingValue) {
        this.keyname = keyname;
        this.joinSortCollector = joinSortCollector;
    }

    @Override
    public int compare(int slot1, int slot2) {
        int result = this.joinSortCollector.comparator.compare(slot1, slot2);
       return result;
    }

    @Override
    public void setBottom(int slot) {
        this.joinSortCollector.comparator.setBottom(slot);
    }

    @Override
    public void setTopValue(BytesRef value) {
        this.joinSortCollector.comparator.setTopValue(value);
    }

    @Override
    public int compareBottom(int doc) throws IOException {
        int key = this.keyValuesArray[doc];
        doc = this.joinSortCollector.setNextReaderForKey(key);
        return this.joinSortCollector.comparator.compareBottom(doc);
    }

    @Override
    public int compareTop(int doc) throws IOException {
        int key = this.keyValuesArray[doc];
        doc = this.joinSortCollector.setNextReaderForKey(key);
        return this.joinSortCollector.comparator.compareTop(doc);
    }

    @Override
    public void copy(int slot, int doc) throws IOException {
        int key = this.keyValuesArray[doc];
        doc = this.joinSortCollector.setNextReaderForKey(key);
        this.joinSortCollector.comparator.copy(slot, doc);
    }

    @Override
    public FieldComparator<BytesRef> setNextReader(AtomicReaderContext context) throws IOException {
        this.keyValuesArray = KeyValuesCache.get(context, this.keyname);
        return this;
    }

    @Override
    public BytesRef value(int slot) {
        return this.joinSortCollector.comparator.value(slot);
    }

}

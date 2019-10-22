/* begin license *
 *
 * "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
 *
 * Copyright (C) 2014-2016, 2019 Seecr (Seek You Too B.V.) https://seecr.nl
 * Copyright (C) 2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
 * Copyright (C) 2015-2016, 2019 Koninklijke Bibliotheek (KB) http://www.kb.nl
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
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.Scorable;


public class DeDupFilterSuperCollector extends SuperCollector<DeDupFilterSubCollector> {
    private final String keyName;
    private final String sortByFieldNames[];
    private final SuperCollector<?> delegate;
    private ConcurrentHashMap<Long, AtomicReference<DeDupFilterSuperCollector.Key>> keys = new ConcurrentHashMap<>();
    private IndexReaderContext topLevelReaderContext = null;

    public DeDupFilterSuperCollector(String keyName, String sortByFieldNames[], SuperCollector<?> delegate) {
        super();
        this.keyName = keyName;
        if (sortByFieldNames ==null) {
            this.sortByFieldNames = new String[0];
        }
        else {
            this.sortByFieldNames = sortByFieldNames;
        }
        this.delegate = delegate;
    }

    public String getKeyName() {
        return this.keyName;
    }

    public int getTotalHits() {
        int totalHits = 0;
        for (DeDupFilterSubCollector dsc : this.subs) {
            totalHits += dsc.getTotalHits();
        }
        return totalHits;
    }

    public long adjustTotalHits(long totalHits) {
        for (AtomicReference<DeDupFilterSuperCollector.Key> key : keys.values()) {
            totalHits -= (key.get().count - 1);
        }
        return totalHits;
    }

    @Override
    protected DeDupFilterSubCollector createSubCollector() throws IOException {
        SubCollector delegateSubCollector = this.delegate.subCollector();
        return new DeDupFilterSubCollector(this.keyName, this.sortByFieldNames, delegateSubCollector, this.keys);
    }

    @Override
    public void complete() throws IOException {
        this.delegate.complete();
    }

    public DeDupFilterSuperCollector.Key keyForDocId(int docId) throws IOException {
        if (this.topLevelReaderContext == null) {
            this.topLevelReaderContext = ReaderUtil.getTopLevelContext(super.subs.get(0).context);
        }

        List<LeafReaderContext> leaves = this.topLevelReaderContext.leaves();
        LeafReaderContext context = leaves.get(ReaderUtil.subIndex(docId, leaves));
        NumericDocValues docValues = context.reader().getNumericDocValues(this.keyName);
        if (docValues == null)
            return null;
        if (!docValues.advanceExact(docId - context.docBase)) {
            return null;
        }
        long keyValue = docValues.longValue();
        if (keyValue == 0)
            return null;
        return this.keys.get(keyValue).get();
    }


    public static class Key {
        private long enumeratedKeyValue;
        private int baseId;
        private int docId;
        private long sortByValues[];
        private int count;
        private long deDupKey;

        public Key(long enumeratedKeyValue, int baseId, int docId, long sortByValues[], int count, long deDupKey) {
            this.enumeratedKeyValue = enumeratedKeyValue;
            this.baseId = baseId;
            this.docId = docId;
            this.sortByValues = sortByValues;
            this.count = count;
            this.deDupKey = deDupKey;
        }

        public Key(Key key, long enumeratedKeyValue, int baseId, int docId, long sortByValues[], long deDupKey) {
            this(enumeratedKeyValue, baseId, docId, sortByValues, 1, deDupKey);
            if (key != null) {
                for (int n = 0; n< sortByValues.length; n++) {
                    if (sortByValues[n] > key.sortByValues[n]) {
                        break;
                    }
                    else if (sortByValues[n] < key.sortByValues[n]) {
                        this.enumeratedKeyValue = key.enumeratedKeyValue;
                        this.sortByValues = key.sortByValues;
                        this.baseId = key.baseId;
                        this.docId = key.docId;
                        break;
                    }
                }
                this.count = key.count + 1;
            }
        }

        public long getDeDupKey() {
            return this.deDupKey;
        }

        public int getDocId() {
            return this.docId;
        }

        public int getCount() {
            return this.count;
        }
    }
}


class DeDupFilterSubCollector extends SubCollector {

    private final SubCollector delegate;
    private ConcurrentHashMap<Long, AtomicReference<DeDupFilterSuperCollector.Key>> keys;

    private int currentDocBase;
    private final String keyName;
    private final String sortByFieldNames[];
    private NumericDocValuesRandomAccess sortByDocValues[];
    private NumericDocValuesRandomAccess keyDocValues;
    private int totalHits = 0;
    LeafReaderContext context;

    public DeDupFilterSubCollector(String keyName,
                                   String sortByFieldNames[],
                                   SubCollector delegate,
                                   ConcurrentHashMap<Long, AtomicReference<DeDupFilterSuperCollector.Key>> keys) {
        super();
        this.delegate = delegate;
        this.keys = keys;
        this.keyName = keyName;
        this.sortByFieldNames = sortByFieldNames;
    }

    @Override
    public void doSetNextReader(LeafReaderContext context) throws IOException {
        this.context = context;
        this.delegate.setNextReader(context);
        this.currentDocBase = context.docBase;
        this.keyDocValues = new NumericDocValuesRandomAccess(context.reader(), this.keyName);
        this.sortByDocValues = new NumericDocValuesRandomAccess[sortByFieldNames.length];
        for (int i = 0; i< sortByFieldNames.length; i++) {
            this.sortByDocValues[i] = new NumericDocValuesRandomAccess(context.reader(), sortByFieldNames[i]);
        }
    }

    @Override
    public void collect(int docId) throws IOException {
        this.totalHits++;
        long keyValue = this.keyDocValues.get(docId);
        if (keyValue > 0) {
            countDocForKey(docId, keyValue);
        }
        this.delegate.collect(docId);
    }

    @Override
    public void setScorer(Scorable s) throws IOException {
        this.delegate.setScorer(s);
    }

	private void countDocForKey(int docId, long keyValue) {
		int absDoc = this.currentDocBase + docId;
		long enumeratedKeyValue = this.keyDocValues.get(docId);
		long sortByValues[] = new long[sortByFieldNames.length];
		for (int i = 0; i< sortByFieldNames.length; i++) {
		    sortByValues[i] = this.sortByDocValues[i].get(docId);
        }

		AtomicReference<DeDupFilterSuperCollector.Key> newRef = new AtomicReference<>();
		AtomicReference<DeDupFilterSuperCollector.Key> curRef = this.keys.putIfAbsent(keyValue, newRef);
		if (curRef == null) {
		    curRef = newRef;
		}

		DeDupFilterSuperCollector.Key key;
		DeDupFilterSuperCollector.Key newKey;
		int retryCount = 0;
		while (true) {
		    key = curRef.get();
		    newKey = new DeDupFilterSuperCollector.Key(key, enumeratedKeyValue, this.currentDocBase, absDoc, sortByValues, keyValue);
		    if (curRef.compareAndSet(key, newKey)) {
		        break;
		    }
            retryCount++;
		    if (retryCount > 10000) {
		        System.out.println("More than 10000 tries in DeDupFilterSubCollector.collect.");
		        System.out.flush();
		        throw new RuntimeException("More than 10000 tries in DeDupFilterSubCollector.collect.");
		    }
		}
	}

    @Override
    public void complete() throws IOException {
        this.delegate.complete();
    }

    public int getTotalHits() {
        return this.totalHits;
    }

}

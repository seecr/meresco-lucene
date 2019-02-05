/* begin license *
 *
 * "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
 *
 * Copyright (C) 2014-2016 Seecr (Seek You Too B.V.) http://seecr.nl
 * Copyright (C) 2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
 * Copyright (C) 2015-2016 Koninklijke Bibliotheek (KB) http://www.kb.nl
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

import org.apache.lucene.index.*;
import org.apache.lucene.search.Scorer;

public class DeDupFilterSuperCollector extends SuperCollector<DeDupFilterSubCollector> {

    private final String keyName;
    private final String sortByFieldName[];
    private final SuperCollector<?> delegate;
    private ConcurrentHashMap<Long, AtomicReference<DeDupFilterSuperCollector.Key>> keys = new ConcurrentHashMap<>();
    private IndexReaderContext topLevelReaderContext = null;

    public DeDupFilterSuperCollector(String keyName, String sortByFieldName[], SuperCollector<?> delegate) {
        super();
        this.keyName = keyName;
        this.sortByFieldName = sortByFieldName;
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

    public int adjustTotalHits(int totalHits) {
        for (AtomicReference<DeDupFilterSuperCollector.Key> key : keys.values()) {
            totalHits -= (key.get().count - 1);
        }
        return totalHits;
    }

    @Override
    protected DeDupFilterSubCollector createSubCollector() throws IOException {
        SubCollector delegateSubCollector = this.delegate.subCollector();
        return new DeDupFilterSubCollector(this.keyName, this.sortByFieldName, delegateSubCollector, this.keys);
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
        long keyValue = docValues.get(docId - context.docBase);
        if (keyValue == 0)
            return null;
        return this.keys.get(keyValue).get();
    }

    public static class Key {
        private long enumeratedKeyValue;
        private int baseId;
        private int docId;
        private long sortByValue[];
        private int count;
        private long deDupKey;

        public Key(long enumeratedKeyValue, int baseId, int docId, long sortByValue[], int count, long deDupKey) {
            this.enumeratedKeyValue = enumeratedKeyValue;
            this.baseId = baseId;
            this.docId = docId;
            this.sortByValue = sortByValue;
            this.count = count;
            this.deDupKey = deDupKey;
        }

        public Key(Key key, long enumeratedKeyValue, int baseId, int docId, long sortByValue[], long deDupKey) {
            this(enumeratedKeyValue, baseId, docId, sortByValue, 1, deDupKey);
            if (key != null) {
                for (int n=0; n<sortByValue.length; n++) {
                    if (sortByValue[n] > key.sortByValue[n]) {
                        break;
                    }
                    else if (sortByValue[n] < key.sortByValue[n]) {
                        this.enumeratedKeyValue = key.enumeratedKeyValue;
                        this.sortByValue = key.sortByValue;
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
    private final String sortByFieldName[];
    private NumericDocValues sortByDocValues[];
    private NumericDocValues keyDocValues;
    private int totalHits = 0;
    LeafReaderContext context;

    public DeDupFilterSubCollector(String keyName,
                                   String sortByFieldName[],
                                   SubCollector delegate,
                                   ConcurrentHashMap<Long, AtomicReference<DeDupFilterSuperCollector.Key>> keys) {
        super();
        this.delegate = delegate;
        this.keys = keys;
        this.keyName = keyName;
        this.sortByFieldName = sortByFieldName;
    }

    @Override
    public void doSetNextReader(LeafReaderContext context) throws IOException {
        this.context = context;
        this.delegate.setNextReader(context);
        this.currentDocBase = context.docBase;

        NumericDocValues kv = context.reader().getNumericDocValues(this.keyName);
        if (kv == null)
            kv = DocValues.emptyNumeric();
        this.keyDocValues = kv;

        this.sortByDocValues = new NumericDocValues[sortByFieldName.length];
        for (int i = 0; i< sortByFieldName.length; i++) {
            kv = context.reader().getNumericDocValues(sortByFieldName[i]);
            if (kv == null) {
                kv = DocValues.emptyNumeric();
            }
            this.sortByDocValues[i] = kv;
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

	private void countDocForKey(int docId, long keyValue) {
		int absDoc = this.currentDocBase + docId;
		long enumeratedKeyValue = this.keyDocValues.get(docId);
		long sortByValues[] = new long[sortByFieldName.length];
		for (int i = 0; i< sortByFieldName.length; i++) {
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
    public void setScorer(Scorer scorer) throws IOException {
        this.delegate.setScorer(scorer);
    }

    @Override
    public void complete() throws IOException {
        this.delegate.complete();
    }

    public int getTotalHits() {
        return this.totalHits;
    }

    @Override
    public boolean needsScores() {
        return this.delegate.needsScores();
    }
}

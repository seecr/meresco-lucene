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

import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.Scorer;


public class DeDupFilterSuperCollector extends SuperCollector<DeDupFilterSubCollector> {
    private final String keyName;
    private final String sortByFieldName;
    private final SuperCollector<?> delegate;
    ConcurrentHashMap<Long, AtomicReference<DeDupFilterSuperCollector.Key>> keys = new ConcurrentHashMap<>();
    private IndexReaderContext topLevelReaderContext = null;

    public DeDupFilterSuperCollector(String keyName, String sortByFieldName, SuperCollector<?> delegate) {
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
        if (this.topLevelReaderContext == null)
            this.topLevelReaderContext = ReaderUtil.getTopLevelContext(super.subs.get(0).context);

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
        private int docId;
        private long sortByValue;
        private int count;

        public Key(int docId, long sortByValue, int count) {
            this.docId = docId;
            this.sortByValue = sortByValue;
            this.count = count;
        }

        public Key(Key key, int docId, long sortByValue) {
            this(docId, sortByValue, 1);
            if (key != null) {
                if (key.sortByValue >= sortByValue) {
                    this.sortByValue = key.sortByValue;
                    this.docId = key.docId;
                }
                this.count = key.count + 1;
            }
        }

        public int getDocId() {
            return this.docId;
        }

        public int getCount() {
            return this.count;
        }

        public long getSortByValue() {
            return this.sortByValue;
        }
    }
}


class DeDupFilterSubCollector extends SubCollector {
    private final SubCollector delegate;
    private ConcurrentHashMap<Long, AtomicReference<DeDupFilterSuperCollector.Key>> keys;

    private int currentDocBase;
    private final String keyName;
    private final String sortByFieldName;
    private NumericDocValuesRandomAccess sortByValues;
    private NumericDocValuesRandomAccess keyValues;
    private int totalHits = 0;
    LeafReaderContext context;

    public DeDupFilterSubCollector(String keyName, String sortByFieldName, SubCollector delegate, ConcurrentHashMap<Long, AtomicReference<DeDupFilterSuperCollector.Key>> keys) throws IOException {
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
        this.keyValues = new NumericDocValuesRandomAccess(context.reader(), this.keyName);
        this.sortByValues = new NumericDocValuesRandomAccess(context.reader(), this.sortByFieldName);
        this.delegate.setNextReader(context);
    }

    @Override
    public void collect(int doc) throws IOException {
        this.totalHits++;
        long keyValue = this.keyValues.get(doc);
        if (keyValue > 0) {
            if (countDocForKey(doc, keyValue) != 1) {
                return;
            }
        }
        this.delegate.collect(doc);
    }

	private int countDocForKey(int doc, long keyValue) {
		int absDoc = this.currentDocBase + doc;
		long sortByValue = this.sortByValues.get(doc);

		AtomicReference<DeDupFilterSuperCollector.Key> ref = new AtomicReference<>();
		AtomicReference<DeDupFilterSuperCollector.Key> newRef = this.keys.putIfAbsent(keyValue, ref);
		if (newRef == null) {
		    newRef = ref;
		}

		DeDupFilterSuperCollector.Key key;
		DeDupFilterSuperCollector.Key newKey;
		int count = 0;
		while (true) {
		    count++;
		    key = newRef.get();
		    newKey = new DeDupFilterSuperCollector.Key(key, absDoc, sortByValue);
		    if (newRef.compareAndSet(key, newKey)) {
		        break;
		    }
		    if (count > 10000) {
		        System.out.println("More than 10000 tries in DeDupFilterSubCollector.collect.");
		        System.out.flush();
		        throw new RuntimeException("More than 10000 tries in DeDupFilterSubCollector.collect.");
		    }
		}
		return newKey.getCount();
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

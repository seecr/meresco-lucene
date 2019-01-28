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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.Scorer;

public class DeDupFilterSuperCollector extends SuperCollector<DeDupFilterSubCollector> {

    private final String keyName;
    private final String sortByFieldName1;
    private final String sortByFieldName2;
    private final SuperCollector<?> delegate;
    private ConcurrentHashMap<Long, AtomicReference<DeDupFilterSuperCollector.Key>> keys = new ConcurrentHashMap<>();
    private ConcurrentHashMap<Integer, Long> docIdToWorkId = new ConcurrentHashMap<>();

    public DeDupFilterSuperCollector(String keyName, String sortByFieldName1, String sortByFieldName2, SuperCollector<?> delegate) {
        super();
        this.keyName = keyName;
        this.sortByFieldName1 = sortByFieldName1;
        this.sortByFieldName2 = sortByFieldName2;
        this.delegate = delegate;

        System.out.println("keyName: "+keyName+" sortByNames: "+this.sortByFieldName1+" and "+this.sortByFieldName2);
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
        return new DeDupFilterSubCollector(this.keyName, this.sortByFieldName1, this.sortByFieldName2, delegateSubCollector, this.keys, this.docIdToWorkId);
    }

    @Override
    public void complete() throws IOException {
        this.delegate.complete();
    }

    public DeDupFilterSuperCollector.Key keyForDocId(int docId) throws IOException {
//        if (this.topLevelReaderContext == null)
//            this.topLevelReaderContext = ReaderUtil.getTopLevelContext(super.subs.get(0).context);
//
//        List<LeafReaderContext> leaves = this.topLevelReaderContext.leaves();
//        LeafReaderContext context = leaves.get(ReaderUtil.subIndex(docId, leaves));
//        NumericDocValues docValues = context.reader().getNumericDocValues(this.keyName);
//        if (docValues == null)
//            return null;
//        long keyValue = docValues.get(docId - context.docBase);
//        if (keyValue == 0)
//            return null;
//        return this.keys.get(keyValue).get();
        Long workId = docIdToWorkId.get(docId);
        return workId == null ? null : this.keys.get(workId).get();
    }

    public Long getWorkId(int docId) {
        return docIdToWorkId.get(docId);
    }
    
    public static class Key {
        private long enumeratedKeyValue;
        private int baseId;
        private int docId;
        private long sortByValue1;
        private long sortByValue2;
        private int count;

        public Key(long enumeratedKeyValue, int baseId, int docId, long sortByValue1, long sortByValue2, int count) {
            this.enumeratedKeyValue = enumeratedKeyValue;
            this.baseId = baseId;
            this.docId = docId;
            this.sortByValue1 = sortByValue1;
            this.sortByValue2 = sortByValue2;
            this.count = count;
        }

        public Key(Key key, long enumeratedKeyValue, int baseId, int docId, long sortByValue1, long sortByValue2) {
            this(enumeratedKeyValue, baseId, docId, sortByValue1, sortByValue2, 1);
            if (key != null) {
                if (key.sortByValue1 > sortByValue1) {
                    this.enumeratedKeyValue = key.enumeratedKeyValue;
                    this.sortByValue1 = key.sortByValue1;
                    this.sortByValue2 = key.sortByValue2;
                    this.baseId = key.baseId;
                    this.docId = key.docId;
                }
                else if (key.sortByValue1 == sortByValue1 && key.sortByValue2 > sortByValue2) {
                    this.enumeratedKeyValue = key.enumeratedKeyValue;
                    this.sortByValue1 = key.sortByValue1;
                    this.sortByValue2 = key.sortByValue2;
                    this.baseId = key.baseId;
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

        public long getSortByValue1() {
            return this.sortByValue1;
        }

        public long getSortByValue2() {
            return this.sortByValue2;
        }
    }
}

class DeDupFilterSubCollector extends SubCollector {

    //private final String ENUMERATED_KEY_NAME = "__key__.lh:item.uri";

    private final SubCollector delegate;
    private ConcurrentHashMap<Long, AtomicReference<DeDupFilterSuperCollector.Key>> keys;
    private ConcurrentHashMap<Integer, Long> docIdToWorkId;

    private int currentDocBase;
    private final String keyName;
    private final String sortByFieldName1;
    private final String sortByFieldName2;
    private NumericDocValues sortByValues1;
    private NumericDocValues sortByValues2;
    private NumericDocValues keyValues; // key value == work identifier
    private int totalHits = 0;
    LeafReaderContext context;

    public SubCollector getDelegate() {
        return delegate;
    }

    public DeDupFilterSubCollector(String keyName,
                                   String sortByFieldName1,
                                   String sortByFieldName2,
                                   SubCollector delegate,
                                   ConcurrentHashMap<Long, AtomicReference<DeDupFilterSuperCollector.Key>> keys,
                                   ConcurrentHashMap<Integer, Long> docIdToWorkId) {
        super();
        this.delegate = delegate;
        this.keys = keys;
        this.keyName = keyName;
        this.sortByFieldName1 = sortByFieldName1;
        this.sortByFieldName2 = sortByFieldName2;
        this.docIdToWorkId = docIdToWorkId;
    }

    @Override
    public void doSetNextReader(LeafReaderContext context) throws IOException {
        this.context = context;
        this.delegate.setNextReader(context);
        this.currentDocBase = context.docBase;

        NumericDocValues kv = context.reader().getNumericDocValues(this.keyName);
        if (kv == null)
            kv = DocValues.emptyNumeric();
        this.keyValues = kv;

        kv = null;
        if (this.sortByFieldName1 != null)
            kv = context.reader().getNumericDocValues(this.sortByFieldName1);
        if (kv == null) {
            System.out.println("sortByFieldName1 did not result in DocValues");
            System.out.flush();
            kv = DocValues.emptyNumeric();
        }
        this.sortByValues1 = kv;

        kv = null;
        if (this.sortByFieldName2 != null)
            kv = context.reader().getNumericDocValues(this.sortByFieldName2);
        if (kv == null) {
            System.out.println("sortByFieldName2 did not result in DocValues");
            System.out.flush();
            kv = DocValues.emptyNumeric();
        }
        this.sortByValues2 = kv;
    }

    @Override
    public void collect(int docId) throws IOException {
        this.totalHits++;
        long keyValue = this.keyValues.get(docId);
        if (keyValue > 0) {
            docIdToWorkId.put(currentDocBase+docId, keyValue);
            countDocForKey(docId, keyValue);
        }
        this.delegate.collect(docId);
    }

	private void countDocForKey(int docId, long keyValue) {
		int absDoc = this.currentDocBase + docId;
		long enumeratedKeyValue = this.keyValues.get(docId);
		long sortByValue1 = this.sortByValues1.get(docId);
        long sortByValue2 = this.sortByValues2.get(docId);

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
		    newKey = new DeDupFilterSuperCollector.Key(key, enumeratedKeyValue, this.currentDocBase, absDoc, sortByValue1, sortByValue2);
		    if (curRef.compareAndSet(key, newKey)) {
		        break;
		    }
            retryCount++;
		    System.out.println(retryCount);
		    System.out.flush();
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
        System.err.println("sub total hits: "+totalHits);
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

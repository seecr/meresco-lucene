/* begin license *
 *
 * "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
 *
 * Copyright (C) 2014 Seecr (Seek You Too B.V.) http://seecr.nl
 * Copyright (C) 2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
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

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.Scorer;

public class DeDupFilterSuperCollector extends SuperCollector<DeDupFilterSubCollector> {

    private final String keyName;
    private final String sortByFieldName;
    private final SuperCollector<SubCollector> delegate;
    ConcurrentHashMap<Long, AtomicReference<DeDupFilterSubCollector.Key>> keys = new ConcurrentHashMap<Long, AtomicReference<DeDupFilterSubCollector.Key>>();
    private IndexReaderContext topLevelReaderContext = null;

    public DeDupFilterSuperCollector(String keyName, String sortByFieldName, SuperCollector<SubCollector> delegate) {
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
    protected DeDupFilterSubCollector createSubCollector(AtomicReaderContext context) throws IOException {
        if (this.topLevelReaderContext == null)
            this.topLevelReaderContext = ReaderUtil.getTopLevelContext(context);
        SubCollector delegateSubCollector = this.delegate.subCollector(context);
        return new DeDupFilterSubCollector(context, this.keyName, this.sortByFieldName, delegateSubCollector, this.keys);
    }

    public DeDupFilterSubCollector.Key keyForDocId(int docId) throws IOException {
        List<AtomicReaderContext> leaves = this.topLevelReaderContext.leaves();
        AtomicReaderContext context = leaves.get(ReaderUtil.subIndex(docId, leaves));
        NumericDocValues docValues = context.reader().getNumericDocValues(this.keyName);
        if (docValues == null)
            return null;
        long keyValue = docValues.get(docId - context.docBase);
        if (keyValue == 0)
            return null;
        return this.keys.get(keyValue).get();
    }
}

class DeDupFilterSubCollector extends SubCollector {
    private final SubCollector delegate;
    private ConcurrentHashMap<Long, AtomicReference<DeDupFilterSubCollector.Key>> keys;

    private final int currentDocBase;
    private final String keyName;
    private final String sortByFieldName;
    private final NumericDocValues sortByValues;
    private final NumericDocValues keyValues;

    private int totalHits = 0;

    public DeDupFilterSubCollector(AtomicReaderContext context, String keyName, String sortByFieldName, SubCollector delegate, ConcurrentHashMap<Long, AtomicReference<DeDupFilterSubCollector.Key>> keys) throws IOException {
        super(context);
        this.delegate = delegate;
        this.keys = keys;

        this.keyName = keyName;
        this.sortByFieldName = sortByFieldName;

        this.currentDocBase = context.docBase;
        NumericDocValues kv = context.reader().getNumericDocValues(this.keyName);
        if (kv == null)
            kv = DocValues.emptyNumeric();
        this.keyValues = kv;

        NumericDocValues sbv = null;
        if (this.sortByFieldName != null)
            sbv = context.reader().getNumericDocValues(this.sortByFieldName);
        if (sbv == null)
            sbv = DocValues.emptyNumeric();
        this.sortByValues = sbv;
    }

    @Override
    public void collect(int doc) throws IOException {
        this.totalHits++;
        long keyValue = this.keyValues.get(doc);
        if (keyValue > 0) {
            int absDoc = this.currentDocBase + doc;
            long sortByValue = this.sortByValues.get(doc);

            AtomicReference<DeDupFilterSubCollector.Key> ref = new AtomicReference<DeDupFilterSubCollector.Key>();
            AtomicReference<DeDupFilterSubCollector.Key> newRef = this.keys.putIfAbsent(keyValue, ref);
            if (newRef == null) {
                newRef = ref;
            }

            DeDupFilterSubCollector.Key key;
            DeDupFilterSubCollector.Key newKey;
            while (true) {
                key = newRef.get();
                newKey = new DeDupFilterSubCollector.Key(key, absDoc, sortByValue);
                if (newRef.compareAndSet(key, newKey)) {
                    break;
                }
            }
            if (newKey.count != 1) {
                return;
            }
        }
        this.delegate.collect(doc);
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
        this.delegate.setScorer(scorer);
    }

    @Override
    public boolean acceptsDocsOutOfOrder() {
        return this.delegate.acceptsDocsOutOfOrder();
    }

    @Override
    public void complete() throws IOException {
        this.delegate.complete();
    }

    public int getTotalHits() {
        return this.totalHits;
    }

    public class Key {
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

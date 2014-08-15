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

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.search.Scorer;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class DeDupFilterSuperCollector extends SuperCollector<DeDupFilterSubCollector> {

	public String keyName;
	String sortByFieldName;
	SuperCollector delegate;
	ConcurrentHashMap<Long, DeDupFilterSubCollector.Key> keys = new ConcurrentHashMap<Long, DeDupFilterSubCollector.Key>();
    private IndexReaderContext topLevelReaderContext = null;

	public DeDupFilterSuperCollector(String keyName, SuperCollector delegate) {
		super();
		this.keyName = keyName;
		this.delegate = delegate;
	}

	public DeDupFilterSuperCollector(String keyName, String sortByFieldName, SuperCollector delegate) {
		super();
		this.keyName = keyName;
		this.sortByFieldName = sortByFieldName;
		this.delegate = delegate;
	}

	public int getTotalHits() {
		int totalHits = 0;
		for (DeDupFilterSubCollector dsc : this.subs) {
			totalHits += dsc.totalHits;
		}
		return totalHits;
	}

	@Override
	protected DeDupFilterSubCollector createSubCollector(AtomicReaderContext context) throws IOException {
		if (this.topLevelReaderContext == null)
            this.topLevelReaderContext = ReaderUtil.getTopLevelContext(context);
		SubCollector delegateSubCollector = this.delegate.subCollector(context);
		return new DeDupFilterSubCollector(context, this, delegateSubCollector, this.keys);
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
        return this.keys.get(keyValue);
	}
}

class DeDupFilterSubCollector extends SubCollector {
	private SubCollector delegate;
	private ConcurrentHashMap<Long, DeDupFilterSubCollector.Key> keys;

    private int currentDocBase;
    public String keyName;
    private String sortByFieldName;
    private NumericDocValues sortByValues;
    private NumericDocValues keyValues;

    public int totalHits = 0;


	public DeDupFilterSubCollector(AtomicReaderContext context, DeDupFilterSuperCollector parent, SubCollector delegate, ConcurrentHashMap<Long, DeDupFilterSubCollector.Key> keys) throws IOException {
		super(context);
		this.delegate = delegate;
		this.keys = keys;

		this.keyName = parent.keyName;
		this.sortByFieldName = parent.sortByFieldName;

        this.currentDocBase = context.docBase;
        this.keyValues = context.reader().getNumericDocValues(this.keyName);
        if (this.keyValues == null)
            this.keyValues = DocValues.emptyNumeric();

        this.sortByValues = null;
        if (this.sortByFieldName != null)
            this.sortByValues = context.reader().getNumericDocValues(this.sortByFieldName);
        if (this.sortByValues == null)
            this.sortByValues = DocValues.emptyNumeric();
	}

	@Override
    public void collect(int doc) throws IOException {
        this.totalHits++;
        long keyValue = this.keyValues.get(doc);
        if (keyValue > 0) {
            int absDoc = this.currentDocBase + doc;
            long sortByValue = this.sortByValues.get(doc);
            DeDupFilterSubCollector.Key key = this.keys.get(keyValue);
            DeDupFilterSubCollector.Key value = null;
            if (key == null) {
                key = new DeDupFilterSubCollector.Key(absDoc, sortByValue, 0);
                value = this.keys.putIfAbsent(keyValue, key);
                if (value != null) {
                    key = value;
                }
            }
            key.count.incrementAndGet();
            long oldSortByValue = key.sortByValue.get();
            int oldDocId = key.docId.get();
            if (oldSortByValue < sortByValue) {
                key.sortByValue.compareAndSet(oldSortByValue, sortByValue);
                key.docId.compareAndSet(oldDocId, absDoc);
            }
            if (value != null) {
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

	public class Key {
	    public AtomicInteger docId;
	    public AtomicLong sortByValue;
	    public AtomicInteger count;

	    public Key(int docId, long sortByValue, int count) {
	        this.docId = new AtomicInteger(docId);
	        this.sortByValue = new AtomicLong(sortByValue);
	        this.count = new AtomicInteger(count);
	    }

	    public int getDocId() {
	        return this.docId.get();
	    }

	    public int getCount() {
	        return this.count.get();
	    }
	}
}

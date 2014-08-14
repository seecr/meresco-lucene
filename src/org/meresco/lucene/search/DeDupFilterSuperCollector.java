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
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopScoreDocCollector;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

public class DeDupFilterSuperCollector extends SuperCollector<DeDupFilterSubCollector> {

	String keyName;
	String sortByFieldName;
	SuperCollector delegate;
	Map<Long, DeDupFilterCollector.Key> keys = new ConcurrentHashMap<Long, DeDupFilterCollector.Key>();

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
		SubCollector delegateSubCollector = this.delegate.subCollector(context);
		return new DeDupFilterSubCollector(context, this, delegateSubCollector, keys);
	}
}

class DeDupFilterSubCollector extends DelegatingSubCollector<DeDupFilterCollector, DeDupFilterSuperCollector> {
	SubCollector subCollector;
	int totalHits;

	public DeDupFilterSubCollector(AtomicReaderContext context, DeDupFilterSuperCollector parent, SubCollector subCollector, Map<Long, DeDupFilterCollector.Key> keys) throws IOException {
		super(context, new DeDupFilterCollector(parent.keyName, parent.sortByFieldName, subCollector, keys), parent);
		this.subCollector = subCollector;
	}

	@Override
	public void complete() throws IOException {
		this.subCollector.complete();
		this.totalHits = this.delegate.totalHits;
	}
}

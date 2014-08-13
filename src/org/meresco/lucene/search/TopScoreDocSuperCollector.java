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

public class TopScoreDocSuperCollector extends SuperCollector<TopScoreDocSubCollector> {

	private int numHits;
	private boolean docsScoredInOrder;

	public TopScoreDocSuperCollector(int numHits, boolean docsScoredInOrder) {
		super();
		this.numHits = numHits;
		this.docsScoredInOrder = docsScoredInOrder;
	}

	@Override
	protected TopScoreDocSubCollector createSubCollector(AtomicReaderContext context) throws IOException {
		return new TopScoreDocSubCollector(context, this, this.numHits, this.docsScoredInOrder);
	}

	public TopDocs topDocs() throws IOException {
		TopDocs[] topdocs = new TopDocs[super.subs.size()];
		for (int i = 0; i < topdocs.length; i++)
			topdocs[i] = super.subs.get(i).topdocs;
		return TopDocs.merge(null, this.numHits, topdocs);
	}
}

class TopScoreDocSubCollector extends DelegatingSubCollector<TopScoreDocCollector, TopScoreDocSuperCollector> {

	TopDocs topdocs;

	public TopScoreDocSubCollector(AtomicReaderContext context, TopScoreDocSuperCollector parent, int numHits, boolean docsScoredInOrder) throws IOException {
		super(context, TopScoreDocCollector.create(numHits, docsScoredInOrder), parent);
	}

	@Override
	public void complete() {
		this.topdocs = this.delegate.topDocs();
	}
}

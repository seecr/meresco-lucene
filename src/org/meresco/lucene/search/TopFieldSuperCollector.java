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
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldCollector;

public class TopFieldSuperCollector extends TopDocSuperCollector<TopFieldSubCollector> {

	protected final Sort sort;
	protected final int numHits;
	protected final boolean fillFields;
	protected final boolean trackDocScores;
	protected final boolean trackMaxScore;
	protected final boolean docsScoredInOrder;

	public TopFieldSuperCollector(Sort sort, int numHits, boolean fillFields, boolean trackDocScores,
			boolean trackMaxScore, boolean docsScoredInOrder) {
		super();
		this.sort = sort;
		this.numHits = numHits;
		this.fillFields = true; // fillFields;
		this.trackDocScores = trackDocScores;
		this.trackMaxScore = trackMaxScore;
		this.docsScoredInOrder = docsScoredInOrder;
	}

	@Override
	protected TopFieldSubCollector createSubCollector(AtomicReaderContext context) throws IOException {
		return new TopFieldSubCollector(context, this);
	}

	public TopDocs topDocs(int start) throws IOException {
		return createTopDocs(start);
	}

	private TopDocs createTopDocs(int start) throws IOException {
		TopDocs[] topdocs = new TopDocs[super.subs.size()];
		for (int i = 0; i < topdocs.length; i++)
			topdocs[i] = super.subs.get(i).topdocs;
		return TopDocs.merge(this.sort, start, this.numHits - start, topdocs);
	}

	public int getTotalHits() throws IOException {
		return createTopDocs(0).totalHits;
	}
}

class TopFieldSubCollector extends DelegatingSubCollector<TopFieldCollector, TopFieldSuperCollector> {

	TopDocs topdocs;
	final AtomicReaderContext context;

	public TopFieldSubCollector(AtomicReaderContext context, TopFieldSuperCollector parent) throws IOException {
		super(context, TopFieldCollector.create(parent.sort, parent.numHits, parent.fillFields, parent.trackDocScores,
				parent.trackMaxScore, parent.docsScoredInOrder), parent);
		this.context = context;
	}

	@Override
	public void complete() {
		this.topdocs = this.delegate.topDocs();
	}
}

class TopFieldSuperScorer extends Scorer {

	private float score;

	protected TopFieldSuperScorer() {
		super(null);
	}

	public void set(float score) {
		this.score = score;
	}

	public float score() {
		return this.score;
	}

	@Override
	public int freq() throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public int docID() {
		throw new UnsupportedOperationException();
	}

	@Override
	public int nextDoc() throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public int advance(int target) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public long cost() {
		throw new UnsupportedOperationException();
	}
}

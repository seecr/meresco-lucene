/* begin license *
 *
 * "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
 *
 * Copyright (C) 2013 Seecr (Seek You Too B.V.) http://seecr.nl
 * Copyright (C) 2013 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
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

package org.meresco.lucene;

import java.io.IOException;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.OpenBitSet;

public class KeyCollector extends Collector {

	private String keyName;
	private NumericDocValues keyValues;
	private OpenBitSet keySet = new OpenBitSet();

	public KeyCollector(String keyName) {
		this.keyName = keyName;
	}

	@Override
	public void collect(int docId) throws IOException {
		this.keySet.set(this.keyValues.get(docId));
	}

	@Override
	public void setNextReader(AtomicReaderContext context) throws IOException {
		this.keyValues = context.reader().getNumericDocValues(this.keyName);
	}

	@Override
	public boolean acceptsDocsOutOfOrder() {
		return true;
	}

	@Override
	public void setScorer(Scorer scorer) throws IOException {}
	
	public OpenBitSet getKeySet() {
		return this.keySet;
	}
	
	public Filter getFilter(final Query q) {
		return new Filter() {
			@Override
			public DocIdSet getDocIdSet(final AtomicReaderContext context, final Bits acceptDocs) throws IOException {
				final AtomicReaderContext topLevelContext = context.reader().getContext();
				final Weight weight = new IndexSearcher(topLevelContext).createNormalizedWeight(q);
				return new DocIdSet() {
					@Override
					public DocIdSetIterator iterator() throws IOException {
						final Scorer scorer = weight.scorer(topLevelContext, true, false, acceptDocs);
						return new DocIdSetIterator() {
							@Override
							public int docID() {
								System.out.println("docId");
								return scorer.docID();
							}
							@Override
							public int nextDoc() throws IOException {
								int docId = scorer.nextDoc();
								while (docId != NO_MORE_DOCS && !KeyCollector.this.keySet.get(docId))
									docId = scorer.nextDoc();
								return docId;
							}
							@Override
							public int advance(int target) throws IOException {
								System.out.println("advance");
								return 0;
							}
							@Override
							public long cost() {
								System.out.println("cost");
								return 1;
							}};
					}};
			}
			
	};
	}
}

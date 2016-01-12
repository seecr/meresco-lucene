/* begin license *
 *
 * "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
 *
 * Copyright (C) 2016 Seecr (Seek You Too B.V.) http://seecr.nl
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

package org.meresco.lucene.suggestion;

import java.io.IOException;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.BitsFilteredDocIdSet;
import org.apache.lucene.util.Bits;


public class SuggestionNGramKeysFilter extends Filter {
	private String keyName;
	public Bits keySet;

	public SuggestionNGramKeysFilter(DocIdSet keySet, String keyName) throws IOException {
		this.keySet = keySet.bits();
		this.keyName = keyName;
	}

	@Override
	public DocIdSet getDocIdSet(final AtomicReaderContext context,
			Bits acceptDocs) throws IOException {
		return BitsFilteredDocIdSet.wrap(new DocIdSet() {
			@Override
			public DocIdSetIterator iterator() throws IOException {
				return new DocIdSetIterator() {
					private BinaryDocValues keysDocValues = FieldCache.DEFAULT.getTerms(context.reader(), keyName, false);
					private int maxDoc = context.reader().maxDoc();
					int docId;

					@Override
					public int docID() {
						throw new UnsupportedOperationException();
					}

					@Override
					public int nextDoc() throws IOException {
						while (this.docId < this.maxDoc) {
							String keys = this.keysDocValues.get(this.docId).utf8ToString();
							for (String key: keys.split("\\|")) {
								if (keySet.get(Integer.parseInt(key))) {
									return this.docId++;
								}
							}
							docId++;
						}
						this.docId = DocIdSetIterator.NO_MORE_DOCS;
						return this.docId;
					}

					@Override
					public int advance(int target) throws IOException {
						this.docId = target;
						return nextDoc();
					}

					@Override
					public long cost() {
						throw new UnsupportedOperationException();
					}
				};
			}
		}, acceptDocs);
	}
}
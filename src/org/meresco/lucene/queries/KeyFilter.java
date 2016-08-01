/* begin license *
 *
 * "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
 *
 * Copyright (C) 2013-2016 Seecr (Seek You Too B.V.) http://seecr.nl
 * Copyright (C) 2013-2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
 * Copyright (C) 2015 Koninklijke Bibliotheek (KB) http://www.kb.nl
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

package org.meresco.lucene.queries;

import java.io.IOException;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.meresco.lucene.search.join.KeyValuesCache;


public class KeyFilter extends Query {
	private String keyName;
	public Bits keySet;

	public KeyFilter(Bits keySet, String keyName) throws IOException {
		this.keySet = keySet;
		this.keyName = keyName;
	}

	@Override
	public Weight createWeight(IndexSearcher searcher, boolean needsScores) {
        return new ConstantScoreWeight(this) {
            @Override
            public Scorer scorer(LeafReaderContext context) throws IOException {
                return new ConstantScoreScorer(this, score(), new DocIdSetIterator() {
                    private int[] keyValuesArray = KeyValuesCache.get(context, keyName);
                    private int maxDoc = context.reader().maxDoc();
                    int docId = -1;

                    @Override
                    public int docID() {
                       return this.docId;
                    }

                    @Override
                    public int nextDoc() throws IOException {
                        this.docId++;
                        if (keyValuesArray != null) {
                            try {
                                while (this.docId < this.maxDoc) {
                                    int key = this.keyValuesArray[this.docId];
                                    if (keySet.get(key)) {
                                        return this.docId;
                                    }
                                    this.docId++;
                                }
                            } catch (IndexOutOfBoundsException e) {
                            }
                        }
                        this.docId = DocIdSetIterator.NO_MORE_DOCS;
                        return this.docId;
                    }

                    @Override
                    public int advance(int target) throws IOException {
                        this.docId = target - 1;
                        return nextDoc();
                    }

                    @Override
                    public long cost() {
                        return 1L;
                    }
               });
           }
       };
    }

    @Override
    public String toString(String field) {
       return "KeyFilter(" + keyName + ")";
    }

    @Override
    public boolean equals(Object o) {
       return sameClassAs(o) && ((KeyFilter) o).keyName == keyName && ((KeyFilter) o).keySet.equals(keySet);
    }

    @Override
    public int hashCode() {
       return classHash();
    }
}
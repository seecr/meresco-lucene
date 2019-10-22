/* begin license *
 *
 * "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
 *
 * Copyright (C) 2016 Koninklijke Bibliotheek (KB) http://www.kb.nl
 * Copyright (C) 2016 Seecr (Seek You Too B.V.) http://seecr.nl
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

package org.meresco.lucene.suggestion;

import java.io.IOException;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;


public class SuggestionNGramKeysFilter extends Query {
	private String keyName;
	public Bits keySet;

	public SuggestionNGramKeysFilter(Bits keySet, String keyName) throws IOException {
		this.keySet = keySet;
		this.keyName = keyName;
	}

	@Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) {
        return new ConstantScoreWeight(this, 1.0f) {
            @Override
            public Scorer scorer(LeafReaderContext context) throws IOException {
                return new ConstantScoreScorer(this, score(), scoreMode, new DocIdSetIterator() {
                    private BinaryDocValues keysDocValues = DocValues.getBinary(context.reader(), keyName);
                    private int maxDoc = context.reader().maxDoc();
                    int docId = -1;

                    @Override
                    public int docID() {
                        return this.docId;
                    }

                    @Override
                    public int nextDoc() throws IOException {
                        this.docId++;
                        while (this.docId < this.maxDoc) {
                            if (!this.keysDocValues.advanceExact(this.docId)) {
                                continue;
                            }
                            String keys = this.keysDocValues.binaryValue().utf8ToString();
                            for (String key: keys.split("\\|")) {
                                if (keySet.get(Integer.parseInt(key))) {
                                    return this.docId;
                                }
                            }
                            this.docId++;
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

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return true;
            }
       };
    }

    @Override
    public String toString(String field) {
       return "SuggestionNGramKeysFilter(" + keyName + ")";
    }

    @Override
    public boolean equals(Object o) {
       return sameClassAs(o) && ((SuggestionNGramKeysFilter) o).keyName == keyName && ((SuggestionNGramKeysFilter) o).keySet.equals(keySet);
    }

    @Override
    public int hashCode() {
       return classHash();
    }
}

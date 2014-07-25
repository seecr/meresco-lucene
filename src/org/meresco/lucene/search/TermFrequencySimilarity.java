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
import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.BytesRef;

public class TermFrequencySimilarity extends Similarity {

    @Override
    public long computeNorm(FieldInvertState state) {
        return (long) (1000 * state.getBoost()); // in milliboosts ;-)
    }

    @Override
    public SimWeight computeWeight(float queryBoost, CollectionStatistics collectionStats, TermStatistics... termStats) {
        return new TermFrequencySimilarityWeight(queryBoost);
    }

    @Override
    public SimScorer simScorer(SimWeight weight, AtomicReaderContext context) throws IOException {
        final TermFrequencySimilarityWeight tfsWeight = (TermFrequencySimilarityWeight) weight;
        return new SimScorer() {

            @Override
            public float score(int doc, float freq) {
                return freq / 1000.0f * tfsWeight.queryBoost;
            }

            @Override
            public float computeSlopFactor(int distance) {
                return 1;
            }

            @Override
            public float computePayloadFactor(int doc, int start, int end, BytesRef payload) {
                return 1;
            }
        };
    }

    class TermFrequencySimilarityWeight extends SimWeight {
        float queryBoost;

        public TermFrequencySimilarityWeight(float queryBoost) {
            this.queryBoost = queryBoost;
        }

        @Override
        public float getValueForNormalization() {
            return 1.0f;
        }

        @Override
        public void normalize(float queryNorm, float topLevelBoost) {
        }
    }

}

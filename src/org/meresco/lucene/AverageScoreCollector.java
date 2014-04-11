/* begin license *
 *
 * "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
 *
 * Copyright (C) 2013-2014 Seecr (Seek You Too B.V.) http://seecr.nl
 * Copyright (C) 2013-2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
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
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.OpenBitSet;


public class AverageScoreCollector extends Collector {
    private ScoreCollector otherScoreCollector;
    private Collector delegate;
    private String keyName;
    private float otherScoreBoost;
    private NumericDocValues keyValues;

    public AverageScoreCollector(String keyName, ScoreCollector otherScoreCollector, float otherScoreBoost) {
        this.keyName = keyName;
        this.otherScoreCollector = otherScoreCollector;
        this.otherScoreBoost = otherScoreBoost;
    }

    public void setDelegate(Collector delegate) {
        this.delegate = delegate;
    }

    @Override
    public void collect(int docId) throws IOException {
        this.delegate.collect(docId);
    }

    @Override
    public void setNextReader(AtomicReaderContext context) throws IOException {
        this.keyValues = context.reader().getNumericDocValues(this.keyName);
        this.delegate.setNextReader(context);
    }

    @Override
    public boolean acceptsDocsOutOfOrder() {
        return this.delegate.acceptsDocsOutOfOrder();
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
        this.delegate.setScorer(new AverageScoreCollector.AverageScorer(scorer));
    }


    class AverageScorer extends Scorer {
        Scorer scorer;

        AverageScorer(Scorer scorer) {
            super(scorer.getWeight());
            this.scorer = scorer;
        }

        public float score() throws IOException {
            float score = this.scorer.score();
            int key = (int) keyValues.get(docID());
            float otherScore = otherScoreCollector.score(key);
            return (score + otherScore * otherScoreBoost) / 2;
        }

        public int freq() throws IOException {
            return this.scorer.freq();
        }

        public long cost() {
            return this.scorer.cost();
        }

        public int advance(int target) throws IOException {
            return this.scorer.advance(target);
        }

        public int nextDoc() throws IOException {
            return this.scorer.nextDoc();
        }

        public int docID() {
            return this.scorer.docID();
        }
    }
}

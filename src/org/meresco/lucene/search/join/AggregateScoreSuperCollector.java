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

package org.meresco.lucene.search.join;

import java.io.IOException;
import java.util.List;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.Scorer;
import org.meresco.lucene.search.SubCollector;
import org.meresco.lucene.search.SuperCollector;
import org.apache.lucene.search.Weight;

public class AggregateScoreSuperCollector extends SuperCollector<AggregateScoreSubCollector> {

    private final String keyName;
    private final ScoreSuperCollector[] otherScoreCollectors;
    private SuperCollector<?> delegate;

    public AggregateScoreSuperCollector(String keyName, List<ScoreSuperCollector> otherScoreCollectors) {
        this.keyName = keyName;
        this.otherScoreCollectors = otherScoreCollectors.toArray(new ScoreSuperCollector[0]);
    }

    public void setDelegate(SuperCollector<?> delegate) {
        this.delegate = delegate;
    }

    @Override
    protected AggregateScoreSubCollector createSubCollector() throws IOException {
        return new AggregateScoreSubCollector(this.keyName, this.otherScoreCollectors, this.delegate.subCollector());
    }

    @Override
    public void complete() throws IOException {
        this.delegate.complete();
    }

}

class AggregateScoreSubCollector extends SubCollector {

    private final SubCollector delegate;
    private NumericDocValues keyValues;
    private final ScoreSuperCollector[] otherScoreCollectors;
    private String keyName;

    public AggregateScoreSubCollector(String keyName, ScoreSuperCollector[] otherScoreCollectors, SubCollector delegate) throws IOException {
        super();
        this.keyName = keyName;
        this.delegate = delegate;
        this.otherScoreCollectors = otherScoreCollectors;
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
        this.delegate.setScorer(new AggregateSuperScorer(scorer));
    }

    @Override
    public void setNextReader(AtomicReaderContext context) throws IOException {
        this.keyValues = context.reader().getNumericDocValues(keyName);
        this.delegate.setNextReader(context);
    }

    @Override
    public void collect(int doc) throws IOException {
        this.delegate.collect(doc);
    }

    @Override
    public boolean acceptsDocsOutOfOrder() {
        return this.delegate.acceptsDocsOutOfOrder();
    }

    @Override
    public void complete() throws IOException {
        this.delegate.complete();
    }

    private static Weight weightFromScorer(Scorer scorer) {
        try {
            return scorer.getWeight();
        } catch (UnsupportedOperationException e) {
            return null;
        }
    }

    private class AggregateSuperScorer extends Scorer {
        private final Scorer scorer;

        private AggregateSuperScorer(Scorer scorer) {
            super(weightFromScorer(scorer));
            this.scorer = scorer;
        }

        public float score() throws IOException {
            float score = this.scorer.score();
            int key = (int) keyValues.get(docID());
            for (ScoreSuperCollector sc : otherScoreCollectors) {
                float otherScore = sc.score(key);
                score *= (float) (1 + otherScore);
            }
            return score;
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

/* begin license *
 *
 * "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
 *
 * Copyright (C) 2014-2016 Seecr (Seek You Too B.V.) http://seecr.nl
 * Copyright (C) 2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
 * Copyright (C) 2015-2016 Koninklijke Bibliotheek (KB) http://www.kb.nl
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

package org.meresco.lucene.search.join;

import java.io.IOException;
import java.util.List;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.Weight;
import org.meresco.lucene.search.SubCollector;
import org.meresco.lucene.search.SuperCollector;


public class AggregateScoreSuperCollector extends SuperCollector<AggregateScoreSubCollector> {
    private final String keyName;
    private final ScoreSuperCollector[] otherScoreCollectors;
    private SuperCollector<?> delegate;
    private float otherScoreRatio = 0.5f;

    public AggregateScoreSuperCollector(String keyName, List<ScoreSuperCollector> otherScoreCollectors, Float otherScoreRatio) {
        this.keyName = keyName;
        this.otherScoreCollectors = otherScoreCollectors.toArray(new ScoreSuperCollector[0]);
        if (otherScoreRatio != null) {
            float f = otherScoreRatio.floatValue();
            if (0.0f <= f && f <= 1.0f) {
                this.otherScoreRatio = f;
            }
        }
    }

    public void setDelegate(SuperCollector<?> delegate) {
        this.delegate = delegate;
    }

    @Override
    protected AggregateScoreSubCollector createSubCollector() throws IOException {
        return new AggregateScoreSubCollector(this.keyName, this.otherScoreCollectors, this.delegate.subCollector(), this.otherScoreRatio);
    }

    @Override
    public void complete() throws IOException {
        this.delegate.complete();
    }

}


class AggregateScoreSubCollector extends SubCollector {
    private final SubCollector delegate;
    private final ScoreSuperCollector[] otherScoreCollectors;
    private String keyName;
    private int[] keyValues;
    private AggregateSuperScorer scorer;
    private float otherScoreRatio;

    public AggregateScoreSubCollector(String keyName, ScoreSuperCollector[] otherScoreCollectors, SubCollector delegate, float otherScoreRatio)
            throws IOException {
        super();
        this.keyName = keyName;
        this.delegate = delegate;
        this.otherScoreCollectors = otherScoreCollectors;
        this.otherScoreRatio = otherScoreRatio;
    }

    @Override
    public void doSetNextReader(LeafReaderContext context) throws IOException {
        this.keyValues = KeyValuesCache.get(context, keyName);
        if (this.scorer != null)
            this.scorer.setKeyValues(this.keyValues);
        this.delegate.setNextReader(context);
    }

    @Override
    public void setScorer(Scorable s) throws IOException {
        this.delegate.setScorer(s);
    }

    @Override
    public void collect(int doc) throws IOException {
        this.delegate.collect(doc);
    }

    @Override
    public void complete() throws IOException {
        this.delegate.complete();
    }
}


class AggregateSuperScorer extends Scorer {
    private final Scorer scorer;
    private int[] keyValues;
    private final ScoreSuperCollector[] otherScoreCollectors;
    private float otherScoreRatio;  // a value between 0.0f and 1.0f

    AggregateSuperScorer(Scorer scorer, ScoreSuperCollector[] otherScoreCollectors, int[] keyValues, float otherScoreRatio) {
        super(weightFromScorer(scorer));
        this.scorer = scorer;
        this.otherScoreCollectors = otherScoreCollectors;
        this.keyValues = keyValues;
        this.otherScoreRatio = otherScoreRatio;
    }

    public void setKeyValues(int[] keyValues) {
        this.keyValues = keyValues;
    }

    @Override
    public float score() throws IOException {
        float score = 1.0f;
        int docId = this.docID();
        int key = this.keyValues != null ? this.keyValues[docId] : 0;
        for (ScoreSuperCollector sc : this.otherScoreCollectors) {
            float otherScore = sc.score(key);
            score *= (1 + otherScore);
        }

        /*
         * Note: the following score weighing applies to one AggregateScoreSuperCollector at a time.
         * In other words: behaviour will (probably) not be as expected when multiple AggregateScoreSuperCollector instances play a part
         * (which is currently the case when multiple 'key' fields on the 'result core' come into play for one query).
         */
        return (1.0f - this.otherScoreRatio) * this.scorer.score() + this.otherScoreRatio * score;
    }

    @Override
    public int docID() {
        return this.scorer.docID();
    }

    private static Weight weightFromScorer(Scorer scorer) {
        try {
            return scorer.getWeight();
        } catch (UnsupportedOperationException e) {
            return null;
        }
    }

    @Override
    public DocIdSetIterator iterator() {
        return this.scorer.iterator();
    }

    @Override
    public float getMaxScore(int upTo) throws IOException {
        return 1.0f;
    }
}

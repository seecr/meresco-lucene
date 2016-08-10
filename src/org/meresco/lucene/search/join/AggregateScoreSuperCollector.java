/* begin license *
 *
 * "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
 *
 * Copyright (C) 2014-2015 Seecr (Seek You Too B.V.) http://seecr.nl
 * Copyright (C) 2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
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

package org.meresco.lucene.search.join;

import java.io.IOException;
import java.util.List;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.meresco.lucene.search.SubCollector;
import org.meresco.lucene.search.SuperCollector;

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
    private final ScoreSuperCollector[] otherScoreCollectors;
    private String keyName;
    private int[] keyValues;
    private AggregateSuperScorer scorer;

    public AggregateScoreSubCollector(String keyName, ScoreSuperCollector[] otherScoreCollectors, SubCollector delegate)
            throws IOException {
        super();
        this.keyName = keyName;
        this.delegate = delegate;
        this.otherScoreCollectors = otherScoreCollectors;
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
        this.scorer = new AggregateSuperScorer(scorer, this.otherScoreCollectors, this.keyValues);
        this.delegate.setScorer(this.scorer);
    }

    @Override
    public void doSetNextReader(LeafReaderContext context) throws IOException {
        this.keyValues = KeyValuesCache.get(context, keyName);
        if (this.scorer != null)
            this.scorer.setKeyValues(this.keyValues);
        this.delegate.setNextReader(context);
    }

    @Override
    public void collect(int doc) throws IOException {
        this.delegate.collect(doc);
    }

    @Override
    public void complete() throws IOException {
        this.delegate.complete();
    }

    @Override
    public boolean needsScores() {
        return true;
    }
}

class AggregateSuperScorer extends Scorer {
    private final Scorer scorer;
    private int[] keyValues;
    private final ScoreSuperCollector[] otherScoreCollectors;

    AggregateSuperScorer(Scorer scorer, ScoreSuperCollector[] otherScoreCollectors, int[] keyValues) {
        super(weightFromScorer(scorer));
        this.scorer = scorer;
        this.otherScoreCollectors = otherScoreCollectors;
        this.keyValues = keyValues;
    }

    public void setKeyValues(int[] keyValues) {
        this.keyValues = keyValues;
    }

    public float score() throws IOException {
        float score = this.scorer.score();
        int docId = this.docID();
        int key = this.keyValues != null ? this.keyValues[docId] : 0;
        for (ScoreSuperCollector sc : this.otherScoreCollectors) {
            float otherScore = sc.score(key);
            score *= (float) (1 + otherScore);
        }
        return score;
    }

    public int freq() throws IOException {
        return this.scorer.freq();
    }

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
}

/* begin license *
 *
 * "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
 *
 * Copyright (C) 2014, 2016, 2019 Seecr (Seek You Too B.V.) https://seecr.nl
 * Copyright (C) 2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
 * Copyright (C) 2016, 2019 Koninklijke Bibliotheek (KB) http://www.kb.nl
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

import org.meresco.lucene.Utils;

import java.io.IOException;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.util.SmallFloat;
import org.meresco.lucene.search.NumericDocValuesRandomAccess;
import org.meresco.lucene.search.SubCollector;
import org.meresco.lucene.search.SuperCollector;


public class ScoreSuperCollector extends SuperCollector<ScoreSubCollector> {
    final String keyName;
    final BlockingDeque<byte[]> arrayPool = new LinkedBlockingDeque<>();
    private byte[] scores;

    public ScoreSuperCollector(String keyName) {
        this.keyName = keyName;
    }

    public float score(int key) {
        int scoresIndex = key * 2;
        if ((scoresIndex + 1) < this.scores.length) {
            int scoreAsBytes = (this.scores[scoresIndex] & 0xff) << 8 | (this.scores[scoresIndex + 1] & 0xff);
            return Utils.int1120ToFloat(scoreAsBytes);
        }
        return 0;
    }

    @Override
    protected ScoreSubCollector createSubCollector() throws IOException {
        return new ScoreSubCollector(this);
    }

    @Override
    public void complete() {
        mergePool(this.arrayPool.poll());
        this.scores = this.arrayPool.poll();
    }

    public void mergePool(byte[] scores) {
        byte[] other = this.arrayPool.poll();
        while (other != null) {
            scores = resize(scores, other.length);
            for (int i = 0; i < Math.min(scores.length, other.length); i++) {
                if (scores[i] == 0) {
                    scores[i] = other[i];
                }
            }
            other = this.arrayPool.poll();
        }
        this.arrayPool.push(scores);
    }

    static byte[] resize(byte[] a, int newSize) {
        if (newSize <= a.length) {
            return a;
        }
        byte[] dest = new byte[newSize];
        System.arraycopy(a, 0, dest, 0, a.length);
        return dest;
    }
}


class ScoreSubCollector extends SubCollector {
    private byte[] scores = new byte[0];
    private Scorable scorer;
    private NumericDocValuesRandomAccess keyValues;
    private final ScoreSuperCollector parent;

    public ScoreSubCollector(ScoreSuperCollector parent) throws IOException {
        super();
        this.parent = parent;
    }

    @Override
    public void doSetNextReader(LeafReaderContext context) throws IOException {
        this.keyValues = new NumericDocValuesRandomAccess(context.reader(), parent.keyName);
    }

    @Override
    public void setScorer(Scorable s) throws IOException {
        scorer = s;
    }

    @Override
    public void collect(int doc) throws IOException {
        int value = (int) this.keyValues.get(doc);
        if (value > 0) {
            int scoresIndex = value * 2;
            if ((scoresIndex + 1) >= this.scores.length) {
                this.scores = ScoreSuperCollector.resize(this.scores, (int) ((scoresIndex + 2) * 1.25));
            }
            int scoreAsBytes = Utils.floatToInt1120(scorer.score());
            this.scores[scoresIndex] = (byte) ((scoreAsBytes >>> 8) & 0xff);
            this.scores[scoresIndex + 1] = (byte) (scoreAsBytes & 0xff);
        }
    }

    @Override
    public void complete() {
        this.parent.mergePool(this.scores);
    }

}

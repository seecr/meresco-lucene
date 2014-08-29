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

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.SmallFloat;
import org.meresco.lucene.search.SubCollector;
import org.meresco.lucene.search.SuperCollector;

public class ScoreSuperCollector extends SuperCollector<ScoreSubCollector> {

    byte[] scores = new byte[0];
    final String keyName;

    public ScoreSuperCollector(String keyName) {
        this.keyName = keyName;
    }

    public synchronized byte[] resize(byte[] src, int newSize) {
        if (newSize < src.length) {
            return src;
        }
        byte[] dest = new byte[(int) (newSize * 1.25)];
        System.arraycopy(src, 0, dest, 0, src.length);
        return dest;
    }

    public float score(int key) {
        if (key < this.scores.length) {
            return SmallFloat.byte315ToFloat(this.scores[key]);
        }
        return 0;
    }

    @Override
    protected ScoreSubCollector createSubCollector() throws IOException {
        return new ScoreSubCollector(this);
    }
}

class ScoreSubCollector extends SubCollector {
    private Scorer scorer;
    private NumericDocValues keyValues;
    private final ScoreSuperCollector parent;

    public ScoreSubCollector(ScoreSuperCollector parent) throws IOException {
        super();
        this.parent = parent;
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
        this.scorer = scorer;
    }

    @Override
    public void setNextReader(AtomicReaderContext context) throws IOException {
        this.keyValues = context.reader().getNumericDocValues(parent.keyName);
    }

    @Override
    public void collect(int doc) throws IOException {
        if (this.keyValues != null) {
            int value = (int) this.keyValues.get(doc);
            if (value > 0) {
                if (value >= this.parent.scores.length) {
                    this.parent.scores = this.parent.resize(this.parent.scores, value + 1);
                }
                this.parent.scores[value] = SmallFloat.floatToByte315(scorer.score());
            }
        }
    }

    @Override
    public boolean acceptsDocsOutOfOrder() {
        return true;
    }

    @Override
    public void complete() {
    }
}
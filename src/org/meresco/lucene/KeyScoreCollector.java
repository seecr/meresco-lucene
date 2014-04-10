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
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.OpenBitSet;
import org.apache.lucene.util.SmallFloat;


public class KeyScoreCollector extends Collector {
    private String keyName;
    private NumericDocValues keyValues;
    protected OpenBitSet keySet = new OpenBitSet();
    protected byte[] scores;
    private Scorer scorer;
    private int docBase;

    public KeyScoreCollector(String keyName) {
        this.keyName = keyName;
    }

    @Override
    public void collect(int docId) throws IOException {
        if (this.keyValues != null) {
            int value = (int)this.keyValues.get(docId);
            if (value > 0) {
                this.keySet.set(value);
                this.scores[value] = SmallFloat.floatToByte315(scorer.score());
            }
        }
    }

    @Override
    public void setNextReader(AtomicReaderContext context) throws IOException {
        if (this.scores == null) {
            IndexReaderContext topLevelContext = context;
            while (!topLevelContext.isTopLevel) {
                topLevelContext = topLevelContext.parent;
            }
            // this.scores = new float[topLevelContext.reader().maxDoc()];
            this.scores = new byte[100];
        }
        this.docBase = context.docBase;
        this.keyValues = context.reader().getNumericDocValues(this.keyName);
    }

    @Override
    public boolean acceptsDocsOutOfOrder() {
        return true;
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
        this.scorer = scorer;
    }

    public float score(int key) {
        return SmallFloat.byte315ToFloat(this.scores[key]);
    }

    public OpenBitSet getCollectedKeys() {
        return this.keySet;
    }
}

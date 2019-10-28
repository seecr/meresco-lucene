/* begin license *
 *
 * "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
 *
 * Copyright (C) 2013-2014, 2016 Seecr (Seek You Too B.V.) http://seecr.nl
 * Copyright (C) 2013-2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
 * Copyright (C) 2016 Koninklijke Bibliotheek (KB) http://www.kb.nl
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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.util.FixedBitSet;
import org.meresco.lucene.search.SubCollector;

public class KeyCollector extends SubCollector {
    protected String keyName;
    private int[] keyValuesArray;
    protected FixedBitSet currentKeySet = new FixedBitSet(0);
    protected int biggestKeyFound = 0;

    public KeyCollector(String keyName) {
        this.keyName = keyName;
    }

    @Override
    public void collect(int docId) throws IOException {
        if (this.keyValuesArray != null) {
            int value = this.keyValuesArray[docId];
            if (value > 0) {
                if (value > this.biggestKeyFound) {
                    this.biggestKeyFound = value;
                    this.currentKeySet = FixedBitSet.ensureCapacity(this.currentKeySet, value + 1);
                }
                this.currentKeySet.set(value);
            }
        }
    }

    @Override
    public void doSetNextReader(LeafReaderContext context) throws IOException {
        keyValuesArray = KeyValuesCache.get(context, keyName);
    }

    public FixedBitSet getCollectedKeys() throws IOException {
        return this.currentKeySet;
    }

    @Override
    public void complete() throws IOException {
    }

    @Override
    public ScoreMode scoreMode() {
        return ScoreMode.COMPLETE_NO_SCORES;
    }
}

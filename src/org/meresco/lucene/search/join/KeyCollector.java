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

package org.meresco.lucene.search.join;

import java.io.IOException;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.OpenBitSet;
import org.meresco.lucene.search.SubCollector;


public class KeyCollector extends SubCollector {
    protected String keyName;
    private NumericDocValues keyValues;
    private int[] keyValuesArray;
    protected OpenBitSet currentKeySet = new OpenBitSet();
    protected int biggestKeyFound = 0;

    public KeyCollector(String keyName) {
        this.keyName = keyName;
    }

    @Override
    public void collect(int docId) throws IOException {
        if (this.keyValues != null) {
        	int value = this.keyValuesArray[docId];
        	if (value == 0) {
        		value = this.keyValuesArray[docId] = (int) this.keyValues.get(docId);
        	}
            if (value > 0) {
                this.currentKeySet.set(value);
                if (value > this.biggestKeyFound) {
                    this.biggestKeyFound = value;
                }
            }
        }
    }

    @Override
    public void setNextReader(AtomicReaderContext context) throws IOException {
        this.keyValues = context.reader().getNumericDocValues(this.keyName);
		keyValuesArray = KeyValuesCache.get(context, keyName);
    }

    @Override
    public boolean acceptsDocsOutOfOrder() {
        return true;
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
    }

    public DocIdSet getCollectedKeys() throws IOException {
        return this.currentKeySet;
    }

	@Override
	public void complete() throws IOException {
	}
}

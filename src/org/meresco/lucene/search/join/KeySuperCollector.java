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
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.OpenBitSet;
import org.meresco.lucene.search.SubCollector;
import org.meresco.lucene.search.SuperCollector;

public class KeySuperCollector extends SuperCollector<KeySubCollector> {
    private final String keyName;
    public OpenBitSet keySet;

    public KeySuperCollector(String keyName) {
        this.keyName = keyName;
    }

    @Override
    protected KeySubCollector createSubCollector() throws IOException {
        return new KeySubCollector(this.keyName);
    }

    public OpenBitSet getCollectedKeys() {
        if (keySet == null) {
            OpenBitSet keySet = super.subs.get(0).keySet;
            for (int i = 1; i < super.subs.size(); i++) {
                keySet.or(super.subs.get(i).keySet);
            }
            this.keySet = keySet;
        }
        return this.keySet;
    }
}

class KeySubCollector extends SubCollector {
    private NumericDocValues keyValues;
    protected OpenBitSet keySet = new OpenBitSet();
    private String keyName;

    public KeySubCollector(String keyName) throws IOException {
        super();
        this.keyName = keyName;
    }

    @Override
    public boolean acceptsDocsOutOfOrder() {
        return true;
    }

    @Override
    public void collect(int doc) throws IOException {
        if (this.keyValues != null) {
            int value = (int) this.keyValues.get(doc);
            if (value > 0) {
                this.keySet.set(value);
            }
        }
    }

    @Override
    public void setScorer(Scorer s) throws IOException {
    }

    @Override
    public void setNextReader(AtomicReaderContext context) throws IOException {
        this.keyValues = context.reader().getNumericDocValues(keyName);
    }

    @Override
    public void complete() throws IOException {
    }
}

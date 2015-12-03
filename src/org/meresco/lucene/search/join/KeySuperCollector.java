/* begin license *
 *
 * "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
 *
 * Copyright (C) 2013-2015 Seecr (Seek You Too B.V.) http://seecr.nl
 * Copyright (C) 2013-2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
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

import org.apache.lucene.util.OpenBitSet;
import org.meresco.lucene.search.SuperCollector;

public class KeySuperCollector extends SuperCollector<KeyCollector> {
    protected final String keyName;
    private OpenBitSet currentKeySet;

    public KeySuperCollector(String keyName) {
        this.keyName = keyName;
    }

    @Override
    protected KeyCollector createSubCollector() throws IOException {
        return new KeyCollector(this.keyName);
    }

    @Override
    public void complete() throws IOException {
        OpenBitSet currentKeySet = super.subs.get(0).currentKeySet;
        for (int i = 1; i < super.subs.size(); i++) {
            currentKeySet.or(super.subs.get(i).currentKeySet);
        }
        super.subs.clear();
        this.currentKeySet = currentKeySet;
    }

    public OpenBitSet getCollectedKeys() {
        return this.currentKeySet;
    }
}

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

import org.apache.lucene.facet.taxonomy.LRUHashMap;
import org.meresco.lucene.queries.KeyFilter;

public class KeyFilterCache {

    static LRUHashMap<CacheKey, KeyFilter> keyFilterCache = new LRUHashMap<CacheKey, KeyFilter>(20);

    public static KeyFilter create(KeyCollector keyCollector, String keyName) {
        KeyFilter keyFilter = KeyFilterCache.keyFilterCache.get(new CacheKey(keyCollector, keyName));
        if (keyFilter == null) {
            keyFilter = new KeyFilter(keyCollector, keyName);
        }
        keyFilter.reset();
        return keyFilter;
    }
}

class CacheKey {
    private KeyCollector keyCollector;
    private String keyName;

    public CacheKey(KeyCollector keyCollector, String keyName) {
        this.keyCollector = keyCollector;
        this.keyName = keyName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CacheKey other = (CacheKey) o;
        return this.keyName.equals(other.keyName) && this.keyCollector.equals(other.keyCollector);
    }

    @Override
    public int hashCode() {
        return keyCollector.hashCode() * 31 + keyName.hashCode();
    }

}

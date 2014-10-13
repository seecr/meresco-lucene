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

import java.util.WeakHashMap;

import org.apache.lucene.facet.taxonomy.LRUHashMap;
import org.apache.lucene.util.OpenBitSet;
import org.meresco.lucene.queries.KeyFilter;

public class KeyFilterCache {

    private static WeakHashMap<OpenBitSet, LRUHashMap<String, KeyFilter>> cache = new WeakHashMap<OpenBitSet, LRUHashMap<String, KeyFilter>>();

    public static KeyFilter create(CachingKeyCollector keyCollector, String keyName) {
        OpenBitSet keySet = keyCollector.getCollectedKeys();
        LRUHashMap<String, KeyFilter> keyFilterCache = KeyFilterCache.cache.get(keySet);

        if (keyFilterCache == null) {
            keyFilterCache = new LRUHashMap<String, KeyFilter>(5);
            KeyFilterCache.cache.put(keySet, keyFilterCache);
        }
        KeyFilter keyFilter = keyFilterCache.get(keyName);
        if (keyFilter == null) {
            keyFilter = new KeyFilter(keySet, keyName, keyCollector.query);
            keyFilterCache.put(keyName, keyFilter);
        }
        return keyFilter;
    }

     public static void printStats() {
        System.out.println("KeyFilterCache. Entries: " + cache.size());
        for (LRUHashMap<String, KeyFilter> filterCache : cache.values()) {
            for (KeyFilter keyFilterCache : filterCache.values()) {
                keyFilterCache.printDocSetCacheSize();
            }
        }
    }
}

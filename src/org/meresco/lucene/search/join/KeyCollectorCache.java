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
import org.apache.lucene.search.Query;

public class KeyCollectorCache {

    /**
     * Caches KeyCollectors for (Query,keyName) pairs. KeyCollectors can become
     * large, tens of MB.
     */
    private static LRUHashMap<Query, LRUHashMap<String, CachingKeyCollector>> queryCache = new LRUHashMap<Query, LRUHashMap<String, CachingKeyCollector>>(20);

    /**
     * Create a collector that collects keys from a field. It caches the keys on
     * segment level. The collector itself is also cached, as well as its
     * Filters.
     *
     * @param query
     *            The Query this collector will be used with.
     * @param keyName
     *            The name of the field that contains the keys. This field must
     *            refer to a NumericDocValues field containing integer keys.
     */
    public static CachingKeyCollector create(Query query, String keyName) {
        LRUHashMap<String, CachingKeyCollector> collectorCache = KeyCollectorCache.queryCache.get(query);
        if (collectorCache == null) {
            collectorCache = new LRUHashMap<String, CachingKeyCollector>(5);
            KeyCollectorCache.queryCache.put(query, collectorCache);
        }

        CachingKeyCollector keyCollector = collectorCache.get(keyName);
        if (keyCollector == null) {
            keyCollector = new CachingKeyCollector(query, keyName);
            collectorCache.put(keyName, keyCollector);
        }
        return keyCollector;
    }
}
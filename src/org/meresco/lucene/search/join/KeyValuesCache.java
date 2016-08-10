/* begin license *
 *
 * "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
 *
 * Copyright (C) 2014, 2016 Seecr (Seek You Too B.V.) http://seecr.nl
 * Copyright (C) 2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
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
import java.util.HashMap;
import java.util.Map;
import java.util.WeakHashMap;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;

public class KeyValuesCache {
    private static Map<Object, Map<String, CacheValue>> cache = new WeakHashMap<Object, Map<String, CacheValue>>();

    public static int[] get(LeafReaderContext context, String keyName) throws IOException {
        LeafReader reader = context.reader();
        NumericDocValues ndv = reader.getNumericDocValues(keyName);
        if (ndv == null) {
            return null;
        }

        CacheValue cacheValue = safeGet(reader, keyName);
        int[] keyValues = cacheValue.keyValues;
        if (!cacheValue.newValue) {
            return keyValues;
        }
        for (int i = 0; i <  reader.maxDoc(); i++) {
            keyValues[i] = (int) ndv.get(i);
        }
        return keyValues;
    }

    private static synchronized CacheValue safeGet(LeafReader reader, String keyName) {
        Map<String, CacheValue> fieldCache = cache.get(reader.getCoreCacheKey());
        if (fieldCache == null) {
            fieldCache = new HashMap<String, CacheValue>();
            KeyValuesCache.cache.put(reader.getCoreCacheKey(), fieldCache);
        }
        CacheValue cacheValue = fieldCache.get(keyName);
        if (cacheValue == null) {
            int[] keyValues = new int[reader.maxDoc()];
            fieldCache.put(keyName, new CacheValue(keyValues, false));
            cacheValue = new CacheValue(keyValues, true);
        }
        return cacheValue;
    }
}

class CacheValue {
    int[] keyValues;
    boolean newValue;

    CacheValue(int[] keyValues, boolean newValue) {
        this.keyValues = keyValues;
        this.newValue = newValue;
    }
}
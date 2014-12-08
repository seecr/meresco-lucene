package org.meresco.lucene.search.join;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.WeakHashMap;

import org.apache.lucene.index.AtomicReaderContext;

public class KeyValuesCache {
    private static Map<Object, Map<String, int[]>> cache = new WeakHashMap<Object, Map<String, int[]>>();

    public static int[] get(AtomicReaderContext context, String keyName) {
        int[] keyValues = safeGet(context, keyName);
        
        return keyValues;
    }

    private static synchronized int[] safeGet(AtomicReaderContext context, String keyName) {
        Map<String, int[]> fieldCache = cache.get(context.reader().getCoreCacheKey());
        if (fieldCache == null) {
            fieldCache = new HashMap<String, int[]>();
            KeyValuesCache.cache.put(context.reader().getCoreCacheKey(), fieldCache);
        }
        int[] keyValues = fieldCache.get(keyName);
        if (keyValues == null) {
            keyValues = new int[context.reader().maxDoc()];
            fieldCache.put(keyName, keyValues);
        }
        return keyValues;
    }
}
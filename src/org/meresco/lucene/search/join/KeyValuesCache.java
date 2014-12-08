package org.meresco.lucene.search.join;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.WeakHashMap;

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.NumericDocValues;

public class KeyValuesCache {
    private static Map<Object, Map<String, CacheValue>> cache = new WeakHashMap<Object, Map<String, CacheValue>>();

    public static int[] get(AtomicReaderContext context, String keyName) throws IOException {
        AtomicReader reader = context.reader();
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

    private static synchronized CacheValue safeGet(AtomicReader reader, String keyName) {
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
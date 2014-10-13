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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;

import org.apache.lucene.facet.taxonomy.LRUHashMap;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.OpenBitSet;
import org.meresco.lucene.queries.KeyFilter;


/**
 * A KeyCollector for implementing joins between two or more Lucene indexs. This
 * version caches the keys and the filters.
 *
 * First use this collector to collect keys from a field containing integers
 * keys in a NumericDocValues field. Then get a Filter with getFilter() and use
 * this to filter on keys in another index.
 *
 * @author erik@seecr.nl
 * */
public class CachingKeyCollector extends KeyCollector {
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
        LRUHashMap<String, CachingKeyCollector> collectorCache = CachingKeyCollector.queryCache.get(query);
        CachingKeyCollector keyCollector = null;
        if (collectorCache != null) {
            keyCollector = collectorCache.get(keyName);
        }
        if (keyCollector == null) {
            keyCollector = new CachingKeyCollector(query, keyName);
        }
        keyCollector.reset();
        return keyCollector;
    }

    private static void putInCache(CachingKeyCollector keyCollector) {
        LRUHashMap<String, CachingKeyCollector> collectorCache = CachingKeyCollector.queryCache.get(keyCollector.query);
        if (collectorCache == null) {
            collectorCache = new LRUHashMap<String, CachingKeyCollector>(5);
            CachingKeyCollector.queryCache.put(keyCollector.query, collectorCache);
        }

        if (!collectorCache.containsKey(keyCollector.keyName)) {
            collectorCache.put(keyCollector.keyName, keyCollector);
        }
    }

    /**
     * Get a BitSet containing all the collected keys. This returns a new
     * BitSet. Use {@link getFilter} with
     * {@link org.apache.lucene.queries.BooleanFilter} or
     * {@link org.apache.lucene.queries.ChainedFilter} for combining
     * results from different collectors to do cross-filtering.
     */
    @Override
    public OpenBitSet getCollectedKeys() {
        updateCache();
        OpenBitSet keySet = new OpenBitSet();
        for (OpenBitSet b : this.seen)
            keySet.or(b);
        return keySet;
    }

    /**
     * Create a Lucene Filter for filtering docs based on a key in this
     * KeyCollector. It can be used for any index, as long as the keyName is
     * correct.
     *
     * @param keyName
     *            The name of the field containing the keys to match. This field
     *            must refer to a NumericDocValues field containing integer
     *            keys.
     * @return A Lucene Filter object. It is cached by this KeyCollector and it
     *         caches the docIds of the index is is applied to.
     */
    public KeyFilter getFilter(String keyName) {
        CachingKeyCollector.putInCache(this);
        KeyFilter filter = this.keyFilterCache.get(keyName);
        if (filter == null) {
            filter = new KeyFilter(this, keyName);
            this.keyFilterCache.put(keyName, filter);
        }
        filter.reset();
        return filter;
    }

    @Override
    public void setNextReader(AtomicReaderContext context) throws IOException {
        updateCache();
        Object readerKey = context.reader().getCombinedCoreAndDeletesKey();
        OpenBitSet bitSet = this.keySetCache.get(readerKey);
        if (bitSet != null) {
            this.seen.add(bitSet);
            throw new CollectionTerminatedException(); // already have this one
        }
        super.setNextReader(context);
        this.currentReaderKey = readerKey;
        this.keySet = new OpenBitSet();
    }

    public void printKeySetCacheSize() {
        int size = 0;
        for (OpenBitSet b : this.keySetCache.values())
            size += b.size();
        System.out.print("cache: " + this.keySetCache.size() + " entries, " + (size / 8 / 1024 / 1024) + " MB");
    }

    /**
     * Caches KeyCollectors for (Query,keyName) pairs. KeyCollectors can become
     * large, tens of MB.
     */
    private static LRUHashMap<Query, LRUHashMap<String, CachingKeyCollector>> queryCache = new LRUHashMap<Query, LRUHashMap<String, CachingKeyCollector>>(20);

    /**
     * Caches bitsets (containing keys) for specific readers within one index.
     * Entries disappear when readers are closed and garbage collected. Note
     * that keys are quasi-randomly distributed, so all bitsets are of the same
     * size, and the whole cache may become large.
     */
    private Map<Object, OpenBitSet> keySetCache = new WeakHashMap<Object, OpenBitSet>();

    /**
     * Caches Lucene Filters for for different keys. The filter can be used on
     * any index as long as the keyName refers to te proper field containing the
     * keys, so this cache may contain readers from different indexes.
     */
    private Map<String, KeyFilter> keyFilterCache = new LRUHashMap<String, KeyFilter>(5);

    /**
     * Records which BitSets belong to the result of the most recent collect
     * cycle (The cache might contain more; for readers not yet GC'd.
     */
    private List<OpenBitSet> seen = new ArrayList<OpenBitSet>();

    /**
     * This indicates the reader for which collection is going on. At the end,
     * this key is used to populate the cache.
     */
    private Object currentReaderKey = null;

    private Query query;

    private CachingKeyCollector(Query query, String keyName) {
        super(keyName);
        this.query = query;
    }

    private void reset() {
        this.currentReaderKey = null;
        this.seen.clear();
    }

    private void updateCache() {
        if (this.currentReaderKey != null) {
            this.keySet.trimTrailingZeros();
            this.keySetCache.put(this.currentReaderKey, this.keySet);
            this.seen.add(this.keySet);
            this.currentReaderKey = null;
        }
    }
}

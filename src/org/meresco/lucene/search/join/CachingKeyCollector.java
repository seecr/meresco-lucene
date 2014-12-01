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

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.OpenBitSet;
import org.apache.lucene.util.PForDeltaDocIdSet;


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
     * Caches bitsets (containing keys) for specific readers within one index.
     * Entries disappear when readers are closed and garbage collected. Note
     * that keys are quasi-randomly distributed, so all bitsets are of the same
     * size, and the whole cache may become large.
     */
    private Map<Object, DocIdSet> keySetCache = new WeakHashMap<Object, DocIdSet>();

    /**
     * Records which BitSets belong to the result of the most recent collect
     * cycle (The cache might contain more; for readers not yet GC'd.
     */
    private List<DocIdSet> seen = new ArrayList<DocIdSet>();

    private FixedBitSet finalKeySet = null;

    private Query query;
    private Object readerKey;
    private Object cacheKey = new Object();

    public CachingKeyCollector(Query query, String keyName) {
        super(keyName);
        this.query = query;
    }

    public Query getQuery() {
    	return this.query;
    }
    
    /**
     * Get a BitSet containing all the collected keys. This returns a new
     * BitSet. Use {@link getFilter} with
     * {@link org.apache.lucene.queries.BooleanFilter} or
     * {@link org.apache.lucene.queries.ChainedFilter} for combining
     * results from different collectors to do cross-filtering.
     */
    @Override
    public DocIdSet getCollectedKeys() throws IOException {
        completePreviousReader();
        if (this.finalKeySet == null) {
            this.finalKeySet = new FixedBitSet(this.biggestKeyFound);
            for (DocIdSet b : this.seen) {
                DocIdSetIterator iterator = b.iterator();
                if (iterator != null) {
                    this.finalKeySet.or(iterator);
                }
            }
        }
        this.seen.clear();
        return this.finalKeySet;
    }

    @Override
    public void setNextReader(AtomicReaderContext context) throws IOException {
        completePreviousReader();
        Object currentReaderKey = context.reader().getCombinedCoreAndDeletesKey();
        DocIdSet bitSet = this.keySetCache.get(currentReaderKey);
        if (bitSet != null) {
            this.seen.add(bitSet);
            throw new CollectionTerminatedException(); // already have this one
        }
        super.setNextReader(context);
        this.readerKey = currentReaderKey;
        this.cacheKey = new Object();
        this.finalKeySet = null;
        this.currentKeySet = new OpenBitSet();
    }

    public void printKeySetCacheSize() {
        int size = 0;
        for (DocIdSet b : this.keySetCache.values()) {
            size += b.ramBytesUsed();
        }
        System.out.println("KeyCollector: query: " + this.query + ", cache: " + this.keySetCache.size() + " entries, " + (size / 1024 / 1024) + " MB");
    }

    private void completePreviousReader() throws IOException {
        if (this.readerKey != null) {
            this.currentKeySet.trimTrailingZeros();
            PForDeltaDocIdSet.Builder builder = new PForDeltaDocIdSet.Builder().add(this.currentKeySet.iterator());
            this.keySetCache.put(readerKey, builder.build());
            this.seen.add(this.currentKeySet);
            this.readerKey = null;
        }
    }

	public Object getCacheKey() {
		return this.cacheKey;
	}

	public void cleanupPreviousCollect() {
		this.seen.clear();
	}
}

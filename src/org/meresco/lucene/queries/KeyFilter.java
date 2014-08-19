/* begin license *
 *
 * "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
 *
 * Copyright (C) 2013 Seecr (Seek You Too B.V.) http://seecr.nl
 * Copyright (C) 2013 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
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

package org.meresco.lucene.queries;

import java.io.IOException;
import java.util.Map;
import java.util.WeakHashMap;

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.OpenBitSet;
import org.meresco.lucene.search.join.CachingKeyCollector;


public class KeyFilter extends Filter {
    private String keyName;
    public OpenBitSet keySet;
    private CachingKeyCollector keyCollector;
    private Map<Object, DocIdSet> docSetCache = new WeakHashMap<Object, DocIdSet>();

    public KeyFilter(CachingKeyCollector cachingKeyCollector, String keyName) {
        this.keyCollector = cachingKeyCollector;
        this.keyName = keyName;
    }

    public void reset() {
        OpenBitSet keySet = this.keyCollector.getCollectedKeys();
        if (keySet.equals(this.keySet))
            return;
        this.keySet = keySet;
        this.docSetCache.clear();
    }

    /**
     * Intersect this filter with another c.q. perform a logical 'and'. This is
     * much faster than using both filters in a BooleanFilter. Note that this
     * changes the internal state until it is reset, which happens when it is
     * (re-)pulled from the cache.
     *
     * @param f
     *            The other KeyFilter that will be intersected. It is not
     *            changed.
     */
    public void intersect(KeyFilter f) {
        this.keySet.and(f.keySet);
    }

    /**
     * Create a union of this filter and another. This performs an logical 'or'.
     * Avoid BooleanFilter and use this method instead, as it is almost three
     * times faster.
     *
     * @param f
     *            The other KeyFilter that will be united. It is not changed.
     */
    public void unite(KeyFilter f) {
        this.keySet.or(f.keySet);
    }

    @Override
    public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException {
        AtomicReader reader = context.reader();
        Object coreKey = reader.getCoreCacheKey();
        DocIdSet docSet = this.docSetCache.get(coreKey);
        if (docSet == null) {
            docSet = this.createDocIdSet(reader);
            this.docSetCache.put(coreKey, docSet);
        }
        return docSet;
    }

    private DocIdSet createDocIdSet(AtomicReader reader) throws IOException {
        NumericDocValues keyValues = reader.getNumericDocValues(this.keyName);
        OpenBitSet docBitSet = new OpenBitSet();
        if (keyValues != null) {
            for (int docId = 0; docId < reader.maxDoc(); docId++) {
                int keyValue = (int) keyValues.get(docId);
                if (keyValue > 0 && this.keySet.get(keyValue)) {
                    docBitSet.set(docId);
                }
            }
        }
        docBitSet.trimTrailingZeros();
        return docBitSet;
    }
}
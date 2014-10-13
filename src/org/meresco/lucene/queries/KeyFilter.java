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
import org.apache.lucene.search.Query;


public class KeyFilter extends Filter {
    private String keyName;
    private Query query;
    public Bits keySet;
    private Map<Object, DocIdSet> docSetCache = new WeakHashMap<Object, DocIdSet>();

    public KeyFilter(DocIdSet keySet, String keyName, Query query) throws IOException {
        this.keySet = keySet.bits();
        this.keyName = keyName;
        this.query = query;
    }

    @Override
    public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException {
        AtomicReader reader = context.reader();
        Object coreKey = reader.getCoreCacheKey();
        DocIdSet docSet = this.docSetCache.get(coreKey);
        if (docSet == null) {
            docSet = this.createDocIdSet(reader);
            synchronized (this) {
                this.docSetCache.put(coreKey, docSet);
            }
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

    public void printDocSetCacheSize() {
        int size = 0;
        for (DocIdSet b : this.docSetCache.values())
            size += b.ramBytesUsed();
        System.out.println("Query: " + this.query + ", cache: " + this.docSetCache.size() + " entries, " + (size / 1024 / 1024) + " MB");
    }

}
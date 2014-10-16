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


public class CachingKeySuperCollector extends KeySuperCollector {

    protected final Query query;
    Map<Object, DocIdSet> keySetCache = new WeakHashMap<Object, DocIdSet>();
    FixedBitSet finalKeySet = null;

    public CachingKeySuperCollector(Query query, String keyName) {
        super(keyName);
        this.query = query;
    }

    @Override
    protected KeySubCollector createSubCollector() throws IOException {
        return new CachingKeySubCollector(this, this.keyName);
    }

    @Override
    public DocIdSet getCollectedKeys() throws IOException {
        List<CachingKeySubCollector> subs = new ArrayList<CachingKeySubCollector>();
        int biggestKeyFound = 0;
        for (KeySubCollector sub : this.subs) {
            CachingKeySubCollector s = (CachingKeySubCollector) sub;
            subs.add(s);
            if (s.biggestKeyFound > biggestKeyFound) {
                biggestKeyFound = s.biggestKeyFound;
            }
        }

        if (this.finalKeySet == null) {
            this.finalKeySet = new FixedBitSet(biggestKeyFound);
            for (CachingKeySubCollector sub : subs) {
                for (DocIdSet b : sub.seen) {
                    DocIdSetIterator iterator = b.iterator();
                    if (iterator != null) {
                        this.finalKeySet.or(iterator);
                    }
                }
            }
        }
        for (CachingKeySubCollector sub : subs) {
           sub.seen.clear();
        }
        return this.finalKeySet;
    }
}

class CachingKeySubCollector extends KeySubCollector {

    private Object readerKey;
    private final CachingKeySuperCollector parent;
    protected List<DocIdSet> seen = new ArrayList<DocIdSet>();

    CachingKeySubCollector(CachingKeySuperCollector parent, String keyName) throws IOException {
        super(keyName);
        this.parent = parent;
    }

    @Override
    public void setNextReader(AtomicReaderContext context) throws IOException {
        completePreviousReader();
        Object currentReaderKey = context.reader().getCombinedCoreAndDeletesKey();
        DocIdSet bitSet = this.parent.keySetCache.get(currentReaderKey);
        if (bitSet != null) {
            this.seen.add(bitSet);
            throw new CollectionTerminatedException(); // already have this one
        }
        super.setNextReader(context);
        this.readerKey = currentReaderKey;
        this.parent.finalKeySet = null;
        this.currentKeySet = new OpenBitSet();
    }

    private void completePreviousReader() throws IOException {
        if (this.readerKey != null) {
            this.currentKeySet.trimTrailingZeros();
            PForDeltaDocIdSet.Builder builder = new PForDeltaDocIdSet.Builder().add(this.currentKeySet.iterator());
            PForDeltaDocIdSet keySet = builder.build();
            synchronized(this) {
                this.parent.keySetCache.put(readerKey, keySet);
            }
            this.seen.add(this.currentKeySet);
            this.readerKey = null;
        }
    }

    @Override
    public void complete() throws IOException {
        completePreviousReader();
    }
}
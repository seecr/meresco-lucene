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

package org.meresco.lucene.search;

import java.io.IOException;
import java.util.Map;
import java.util.WeakHashMap;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.util.OpenBitSet;
import org.apache.lucene.util.Bits;


public class DocIdCollector extends Collector {
    private AtomicReaderContext readerContext = null;
    private Map<Object, OpenBitSet> segmentDocIdSets = new WeakHashMap<Object, OpenBitSet>();
    private OpenBitSet docIdSet = new OpenBitSet();

    @Override
    public void collect(int docId) throws IOException {
        int doc = this.readerContext.docBase + docId;
        this.docIdSet.set(doc);
        this.segmentDocIdSets.get(this.readerContext).set(docId);
    }

    @Override
    public void setNextReader(AtomicReaderContext context) throws IOException {
        this.readerContext = context;
        OpenBitSet segmentDocIdSet = segmentDocIdSets.get(context);
        if (segmentDocIdSet == null) {
            segmentDocIdSet = new OpenBitSet();
        }
        segmentDocIdSets.put(context, segmentDocIdSet);
    }

    @Override
    public boolean acceptsDocsOutOfOrder() {
        return true;
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
    }

    public OpenBitSet getDocIdSet() {
        return this.docIdSet;
    }

    public Filter getDocIdFilter() {
        return new Filter() {
            @Override
            public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException {
                return DocIdCollector.this.segmentDocIdSets.get(context);
            }
        };
    }
}
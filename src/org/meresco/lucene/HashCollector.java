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

package org.meresco.lucene;

import java.io.IOException;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.FieldCache;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;

public class HashCollector extends Collector {

    String fromField;
    FieldCache.Longs fromFieldValues;
    Map<Long, List<Integer>> hashes = new HashMap<Long, List<Integer>>();
    Collector facetCollector;
    // IndexReader toplevel_reader = null;
    int docBase;
    List<AtomicReaderContext> contexts = new ArrayList<AtomicReaderContext>();

    public HashCollector(String fromField, Collector facetCollector) {
        this.fromField = fromField;
        this.facetCollector = facetCollector;
    }

    private AtomicReaderContext contextForDocId(int docId) {
        int index = 0;
        for (AtomicReaderContext context : this.contexts) {
            System.out.println(context.docBase);
            if (docId <= context.docBase) {
                return context;
            }
        }
        return this.contexts.get(this.contexts.size() - 1);
    }

    public boolean contains(long hash) throws IOException {
        if (hashes.containsKey(hash)) {
            List<Integer> absDocIds = hashes.get(hash);
            if (this.facetCollector != null) {
                for (Integer absDocId : absDocIds) {
                    AtomicReaderContext context = contextForDocId(absDocId);
                    this.facetCollector.setNextReader(context);
                    this.facetCollector.collect(absDocId - context.docBase);
                }
            }
            return true;
        }
        return false;
    }

    @Override
    public void collect(int doc) throws IOException {
        long hash = this.fromFieldValues.get(doc);
        int absDocId = doc + this.docBase;
        List<Integer> docIds = hashes.get(hash);
        if (docIds == null) {
            docIds = new ArrayList<Integer>();
            this.hashes.put(hash, docIds);
        }
        docIds.add(absDocId);
    }

    @Override
    public void setNextReader(AtomicReaderContext context) throws IOException {
        this.docBase = context.docBase;
        this.contexts.add(context);
        // if (this.toplevel_reader == null) {
        //     IndexReaderContext c = context;
        //     while (!c.isTopLevel) {
        //         c = c.parent;
        //     }
        //     this.toplevel_reader = c.reader();
        // }
        this.fromFieldValues = FieldCache.DEFAULT.getLongs(context.reader(), this.fromField, false);
        // if (this.facetCollector != null) {
        //     this.facetCollector.setNextReader(context);
        // }
    }

    @Override
    public boolean acceptsDocsOutOfOrder() {
        return false;
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {}

}
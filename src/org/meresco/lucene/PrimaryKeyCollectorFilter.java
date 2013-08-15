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
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.OpenBitSet;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.ArrayList;

public class PrimaryKeyCollectorFilter extends Collector {

    String primaryKeyName;
    Collector nextCollector = null;
    Scorer scorer;
    HashScorer hashScorer = new HashScorer();
    FieldCache.Longs primaryKeyValues;
    Map<Long, Integer> hashesToDocId = new HashMap<Long, Integer>();
    Map<Integer, Float> docIdToScore = new HashMap<Integer, Float>();

    Boolean shouldFilter;

    int docBase;
    int nextDocBase = 0;
    List<AtomicReaderContext> contexts = new ArrayList<AtomicReaderContext>();
    List<Integer> docBases = new ArrayList<Integer>();

    OpenBitSet docSet = new OpenBitSet();

    public PrimaryKeyCollectorFilter(String primaryKeyName, Boolean shouldFilter) {
        this.primaryKeyName = primaryKeyName;
        this.shouldFilter = shouldFilter; // Should not filter based on foreign key in contains (For example with only joinFacets)
    }

    public void setNextCollector(Collector nextCollector) throws IOException {
        this.nextCollector = nextCollector;
        this.nextCollector.setScorer(this.hashScorer);
    }

    private void setContextForDocId(int docId) throws IOException {
        if (docId < this.nextDocBase || this.nextDocBase == -1) {
            return;
        }
        int index = -1;
        this.nextDocBase = -1;
        for (Integer docBase : this.docBases) {
            if (docBase > docId) {
                this.nextDocBase = docBase;
                break;
            }
            index++;
        }
        AtomicReaderContext context = this.contexts.get(index);
        this.docBase = context.docBase;
        this.nextCollector.setNextReader(context);
        // System.out.println("DocId: " + docId + "; Set to docBase: " + this.docBase);
    }

    public boolean contains(long hash) throws IOException {
        Integer absDocId = hashesToDocId.get(hash);
        if (absDocId != null) {
            if (this.nextCollector != null && this.shouldFilter) {
                docSet.set(absDocId);
            }
            return true;
        }
        return false;
    }

    public void finishCollecting() throws IOException {
        DocIdSetIterator it = this.docSet.iterator();
        int docId;
        while ((docId = it.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
            this.setContextForDocId(docId);
            this.hashScorer.setDocId(docId);
            this.nextCollector.collect(docId - this.docBase);
        }
    }

    @Override
    public void collect(int doc) throws IOException {
        long hash = this.primaryKeyValues.get(doc);
        int absDocId = doc + this.docBase;
        this.hashesToDocId.put(hash, absDocId);
        this.docIdToScore.put(absDocId, this.scorer.score());
        if (!this.shouldFilter) {
            docSet.set(absDocId);
        }
    }

    @Override
    public void setNextReader(AtomicReaderContext context) throws IOException {
        this.docBase = context.docBase;
        this.contexts.add(context);
        this.docBases.add(this.docBase);
        this.primaryKeyValues = FieldCache.DEFAULT.getLongs(context.reader(), this.primaryKeyName, false);
    }

    @Override
    public boolean acceptsDocsOutOfOrder() {
        return this.nextCollector.acceptsDocsOutOfOrder();
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
        this.scorer = scorer;
    }

    class HashScorer extends Scorer {
        private int currentDocId;

        public HashScorer() {
            super(null);
        }
        public void setDocId(int docId) {
            this.currentDocId = docId;
        }

        public float score() {
            return docIdToScore.get(this.currentDocId);
        }

        public int freq() throws IOException {return -1;}
        public long cost() {return -1;}
        public int advance(int target) throws IOException {return -1;}
        public int nextDoc() throws IOException {return -1;}
        public int docID() {return -1;}
    }

}
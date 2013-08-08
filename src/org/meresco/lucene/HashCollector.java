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

public class HashCollector extends Collector {

    String primaryKeyName;
    Collector nextCollector = null;
    Scorer scorer;
    HashScorer hashScorer = new HashScorer();
    FieldCache.Longs primaryKeyValues;
    Map<Long, Integer> hashesToDocId = new HashMap<Long, Integer>();
    Map<Integer, Float> docIdToScore = new HashMap<Integer, Float>();

    int docBase;
    int nextDocBase = -1;
    int currentContext = -1;
    List<AtomicReaderContext> contexts = new ArrayList<AtomicReaderContext>();

    OpenBitSet docSet = new OpenBitSet();

    public HashCollector(String primaryKeyName) {
        this.primaryKeyName = primaryKeyName;
    }

    public void setNextCollector(Collector nextCollector) throws IOException {
        this.nextCollector = nextCollector;
        this.nextCollector.setScorer(this.hashScorer);
    }

    private void setContextForDocId(int docId) throws IOException {
        if (docId < this.nextDocBase) {
            return;
        }
        AtomicReaderContext nextContext;
        while (true) {
            nextContext = this.contexts.get(this.currentContext + 1);
            if (nextContext.docBase < docId) {
                this.currentContext++;
                this.nextDocBase = nextContext.docBase;
                if (this.currentContext == this.contexts.size()) {
                    break;
                }
            } else {
                break;
            }
        }
        this.docBase = nextContext.docBase;
        this.nextCollector.setNextReader(nextContext);
    }

    public boolean contains(long hash) throws IOException {
        // System.out.println("Contains hash: " + hash);
        Integer absDocId = hashesToDocId.get(hash);
        if (absDocId != null) {
            if (this.nextCollector != null) {
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
            setContextForDocId(docId);
            this.hashScorer.setDocId(docId);
            // System.out.println("next collector.collect docID: "+ (docId - context.docBase));
            this.nextCollector.collect(docId - this.docBase);
        }
    }

    @Override
    public void collect(int doc) throws IOException {
        long hash = this.primaryKeyValues.get(doc);
        int absDocId = doc + this.docBase;
        this.hashesToDocId.put(hash, absDocId);
        this.docIdToScore.put(absDocId, this.scorer.score());
        // System.out.println("Collect: Hash: " + hash + " docId: " + absDocId);
    }

    @Override
    public void setNextReader(AtomicReaderContext context) throws IOException {
        this.docBase = context.docBase;
        this.contexts.add(context);
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
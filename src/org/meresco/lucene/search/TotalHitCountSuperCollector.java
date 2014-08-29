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

package org.meresco.lucene.search;

import java.io.IOException;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Scorer;

public class TotalHitCountSuperCollector extends SuperCollector<TotalHitCountSubCollector> {

    @Override
    public TotalHitCountSubCollector createSubCollector() {
        return new TotalHitCountSubCollector();
    }

    public int getTotalHits() {
        int n = 0;
        for (TotalHitCountSubCollector sub : super.subs) {
            n += sub.getTotalHits();
        }
        return n;
    }
}

class TotalHitCountSubCollector extends SubCollector {

    private int totalHits = 0;

    @Override
    public void setNextReader(AtomicReaderContext context) {
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
    }

    @Override
    public void collect(int doc) throws IOException {
        this.totalHits++;
    }

    @Override
    public boolean acceptsDocsOutOfOrder() {
        return true;
    }

    public int getTotalHits() {
        return totalHits;
    }

    @Override
    public void complete() {
    }
}

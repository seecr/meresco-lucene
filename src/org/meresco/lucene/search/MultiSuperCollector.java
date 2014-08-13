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
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.search.Scorer;
import java.util.ArrayList;

public class MultiSuperCollector extends SuperCollector<MultiSubCollector> {

    ArrayList<SuperCollector> collectors = new ArrayList<SuperCollector>();

    public MultiSuperCollector() {
        super();
        // this.collectors = collectors;
    }

    public void add(SuperCollector collector) {
        collectors.add(collector);
    }

    @Override
    protected MultiSubCollector createSubCollector(AtomicReaderContext context) throws IOException {
        ArrayList<SubCollector> subCollectors = new ArrayList<SubCollector>();
        for (SuperCollector sc : collectors) {
            subCollectors.add(sc.subCollector(context));
        }
        return new MultiSubCollector(context, subCollectors);
    }
}

class MultiSubCollector extends SubCollector {
    private ArrayList<SubCollector> subCollectors = new ArrayList<SubCollector>();

    public MultiSubCollector(AtomicReaderContext context, ArrayList<SubCollector> subCollectors) throws IOException {
        super(context);
        this.subCollectors = subCollectors;
    }

    @Override
    public boolean acceptsDocsOutOfOrder() {
        for (SubCollector c : this.subCollectors) {
            if (!c.acceptsDocsOutOfOrder()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void collect(int doc) throws IOException {
        for (SubCollector c : this.subCollectors) {
            c.collect(doc);
        }
    }

    @Override
    public void setScorer(Scorer s) throws IOException {
        for (SubCollector c : this.subCollectors) {
            c.setScorer(s);
        }
    }


    @Override
    public void complete() throws IOException {
        for (SubCollector c : this.subCollectors) {
            c.complete();
        }
    }
}
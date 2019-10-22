/* begin license *
 *
 * "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
 *
 * Copyright (C) 2014, 2016 Seecr (Seek You Too B.V.) http://seecr.nl
 * Copyright (C) 2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
 * Copyright (C) 2016 Koninklijke Bibliotheek (KB) http://www.kb.nl
 * Copyright (C) 2016 Stichting Kennisnet http://www.kennisnet.nl
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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;

public abstract class DelegatingSubCollector<CollectorType extends Collector, SuperCollectorType extends SuperCollector<?>> extends SubCollector {

    protected CollectorType delegate;
    protected SuperCollectorType parent;
    private LeafCollector leaf;

    public DelegatingSubCollector(CollectorType delegate, SuperCollectorType parent) throws IOException {
        super();
        this.delegate = delegate;
        this.parent = parent;
    }
    
    @Override
    protected void doSetNextReader(LeafReaderContext context) throws IOException {
        this.leaf = this.delegate.getLeafCollector(context);
    }

    @Override
    public void setScorer(Scorable s) throws IOException {
        this.leaf.setScorer(s);
    }

    @Override
    public void collect(int doc) throws IOException {
        this.leaf.collect(doc);
    }
}

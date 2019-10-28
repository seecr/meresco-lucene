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
import java.util.List;
import java.util.stream.Stream;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;

public class MultiSuperCollector extends SuperCollector<MultiSubCollector> {

    private final SuperCollector<?>[] collectors;

    public MultiSuperCollector(List<SuperCollector<?>> collectors) {
        super();
        this.collectors = collectors.toArray(new SuperCollector[0]);
    }

    public MultiSuperCollector(SuperCollector<?>... c) {
        this.collectors = c;
    }

    @Override
    protected MultiSubCollector createSubCollector() throws IOException {
        SubCollector[] subCollectors = new SubCollector[this.collectors.length];
        int count = 0;
        for (SuperCollector<?> sc : collectors) {
            subCollectors[count++] = sc.subCollector();
        }
        return new MultiSubCollector(subCollectors);
    }

    @Override
    public void complete() throws IOException {
        for (SuperCollector<?> sc : collectors) {
            sc.complete();
        }
    }

}

class MultiSubCollector extends SubCollector {
    private final SubCollector[] subCollectors;

    public MultiSubCollector(SubCollector[] subCollectors) throws IOException {
        super();
        this.subCollectors = subCollectors;
    }

    @Override
    protected void doSetNextReader(LeafReaderContext context) throws IOException {
        for (SubCollector c : this.subCollectors) {
            c.getLeafCollector(context);
        }
    }

    @Override
    public void setScorer(Scorable s) throws IOException {
        for (SubCollector c : this.subCollectors) {
            c.setScorer(s);
        }
    }

    @Override
    public void collect(int doc) throws IOException {
        for (SubCollector c : this.subCollectors) {
            c.collect(doc);
        }
    }

    @Override
    public void complete() throws IOException {
        for (SubCollector c : this.subCollectors) {
            c.complete();
        }
    }

    @Override
    public ScoreMode scoreMode() {
        if (Stream.of(this.subCollectors).anyMatch(c -> (ScoreMode.COMPLETE_NO_SCORES != c.scoreMode()))) {
            return ScoreMode.COMPLETE;
        }
        return ScoreMode.COMPLETE_NO_SCORES;
    }
}

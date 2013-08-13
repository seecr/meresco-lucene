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
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.FieldCache;


public class PrimaryKeyCollectorFilter2 extends Collector {

    String primaryKeyName;
    FieldCache.Longs primaryKeyValues;
    ForeignKeyCollector backwardFilter;
    Collector nextCollector = null;

    public PrimaryKeyCollectorFilter2(ForeignKeyCollector backwardFilter, String primaryKeyName) throws IOException {
        this.backwardFilter = backwardFilter;
        this.primaryKeyName = primaryKeyName;
    }

    public void setNextCollector(Collector nextCollector) {
        this.nextCollector = nextCollector;
    }

    @Override
    public void collect(int doc) throws IOException {
        if (this.backwardFilter.contains(this.primaryKeyValues.get(doc))) {
            if (this.nextCollector != null) {
                this.nextCollector.collect(doc);
            }
        }
    }

    @Override
    public void setNextReader(AtomicReaderContext context) throws IOException {
        this.primaryKeyValues = FieldCache.DEFAULT.getLongs(context.reader(), this.primaryKeyName, false);
        if (this.nextCollector != null) {
            this.nextCollector.setNextReader(context);
        }
    }

    @Override
    public boolean acceptsDocsOutOfOrder() {
        if (this.nextCollector != null) {
           return this.nextCollector.acceptsDocsOutOfOrder();
        }
        return true;
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
        if (this.nextCollector != null) {
            this.nextCollector.setScorer(scorer);
        }
    }

}
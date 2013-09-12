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
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.OpenBitSet;
import org.apache.solr.search.DelegatingCollector;

/**
 * A collector that filters by looking up keys (ords) in a named field.
 */
public class KeyCollectorFilter extends DelegatingCollector {

    private OpenBitSet keyFilter;
	private String keyName;
    private NumericDocValues keyValues;

    public KeyCollectorFilter(OpenBitSet keyFilter, String keyName) throws IOException {
        this.keyFilter = keyFilter;
        this.keyName = keyName;
        this.setDelegate(new NoopCollector());
    }

    @Override
    public void collect(int docId) throws IOException {
        if (this.keyFilter.get(this.keyValues.get(docId)))
            super.collect(docId);
    }

    @Override
    public void setNextReader(AtomicReaderContext context) throws IOException {
        super.setNextReader(context);
        this.keyValues = context.reader().getNumericDocValues(this.keyName);
    }
}

class NoopCollector extends Collector {

	@Override
	public void setScorer(Scorer scorer) throws IOException {}

	@Override
	public void collect(int doc) throws IOException {}

	@Override
	public void setNextReader(AtomicReaderContext context) throws IOException {}

	@Override
	public boolean acceptsDocsOutOfOrder() { return true; }
	
}
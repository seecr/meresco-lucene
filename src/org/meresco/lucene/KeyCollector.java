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

public class KeyCollector extends Collector {

    String foreignKeyName;
    FieldCache.Longs foreignKeyValues;
    HashSetLinear hashes = new HashSetLinear((int) Math.pow(2, 20));

    public KeyCollector(String foreignKeyName) {
        this.foreignKeyName = foreignKeyName;
    }

    public boolean contains(long hash) throws IOException {
        return hashes.contains(hash);
    }

    @Override
    public void collect(int doc) throws IOException {
        this.hashes.add(this.foreignKeyValues.get(doc));
    }

    @Override
    public void setNextReader(AtomicReaderContext context) throws IOException {
        this.foreignKeyValues = FieldCache.DEFAULT.getLongs(context.reader(), this.foreignKeyName, false);
    }

    @Override
    public boolean acceptsDocsOutOfOrder() {
        return true;
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {}
}
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
import java.util.HashSet;
import java.util.Set;

public class HashCollector extends Collector {

    String fromField;
    FieldCache.Longs fromFieldValues;
    Set<Long> hashes = new HashSet<Long>();

    public HashCollector(String fromField) {
        this.fromField = fromField;
    }

    public boolean contains(long hash) {
        return hashes.contains(hash);
    }

    @Override
    public void collect(int doc) throws IOException {
        hashes.add(this.fromFieldValues.get(doc));
    }

    @Override
    public void setNextReader(AtomicReaderContext context) throws IOException {
        this.fromFieldValues = FieldCache.DEFAULT.getLongs(context.reader(), this.fromField, false);
    }

    @Override
    public boolean acceptsDocsOutOfOrder() {
        return true;
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {}

    public void debug() {
        for (Long l : this.hashes) {
            System.out.println(l);
        }
    }

}
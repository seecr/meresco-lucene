/* begin license *
 *
 * "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
 *
 * Copyright (C) 2015 Koninklijke Bibliotheek (KB) http://www.kb.nl
 * Copyright (C) 2015 Seecr (Seek You Too B.V.) http://seecr.nl
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

import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.SortField;

public class JoinSortField extends SortField {

    private JoinSortCollector joinSortCollector;
    private String coreName;

    public JoinSortField(String field, Type type, boolean reverse, String coreName) {
        super(field, type, reverse);
        this.coreName = coreName;
    }
    
    public void setCollector(JoinSortCollector collector) {
        this.joinSortCollector = collector;
    }
    
    public FieldComparator<?> getComparator(final int numHits, final int sortPos) throws IOException {
        return this.joinSortCollector.getComparator(this.getField(), this.getType(), this.getReverse(), numHits, sortPos, this.missingValue);
    }

    public String getCoreName() {
        return this.coreName;
    }
}
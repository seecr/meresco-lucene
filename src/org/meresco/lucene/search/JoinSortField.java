/* begin license *
 *
 * "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
 *
 * Copyright (C) 2015-2016 Koninklijke Bibliotheek (KB) http://www.kb.nl
 * Copyright (C) 2015-2017 Seecr (Seek You Too B.V.) https://seecr.nl
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

    @Override
    public FieldComparator<?> getComparator(final int numHits, final int sortPos) {
        return this.joinSortCollector.getComparator(this.getField(), this.getType(), this.getReverse(), numHits, sortPos, this.missingValue);
    }

    public String getCoreName() {
        return this.coreName;
    }
}


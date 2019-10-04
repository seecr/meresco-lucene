/* begin license *
 *
 * "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
 *
 * Copyright (C) 2015-2016, 2019 Koninklijke Bibliotheek (KB) http://www.kb.nl
 * Copyright (C) 2015-2016, 2019 Seecr (Seek You Too B.V.) http://seecr.nl
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

import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldDocs;


public abstract class TopDocSuperCollector extends SuperCollector<TopDocSubCollector<?>> {
    protected final Sort sort;
    protected final int numHits;

    public TopDocSuperCollector(Sort sort, int numHits) {
        super();
        this.sort = sort;
        this.numHits = numHits;
    }

    public TopDocs topDocs(int start) throws IOException {
        TopDocs[] topdocs;
        if (this.sort==null) {
            topdocs = new TopDocs[this.subs.size()];
            for (int i = 0; i < topdocs.length; i++) {
                topdocs[i] = this.subs.get(i).topdocs;
            }
            return TopDocs.merge(start, numHits - start, topdocs);
        }
        else {
            topdocs = new TopFieldDocs[this.subs.size()];
            for (int i = 0; i < topdocs.length; i++) {
                topdocs[i] = this.subs.get(i).topdocs;
            }
            return TopDocs.merge(this.sort, start, this.numHits - start, (TopFieldDocs[]) topdocs);
        }
    }

    public int getTotalHits() throws IOException {
        int totalHits = 0;
        for (TopDocSubCollector<?> sub : this.subs) {
            totalHits += sub.topdocs.totalHits;
        }
        return totalHits;
    }
}

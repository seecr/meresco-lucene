/* begin license *
 *
 * "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
 *
 * Copyright (C) 2014-2016, 2019 Seecr (Seek You Too B.V.) https://seecr.nl
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

import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopFieldCollector;

public class TopFieldSuperCollector extends TopDocSuperCollector {

    final boolean trackDocScores;
    final boolean trackMaxScore;

    public TopFieldSuperCollector(Sort sort, int numHits, boolean trackDocScores, boolean trackMaxScore) {
        super(sort, numHits);
        this.trackDocScores = trackDocScores;
        this.trackMaxScore = trackMaxScore;
    }

    @Override
    protected TopDocSubCollector<TopFieldSuperCollector> createSubCollector() throws IOException {
        // Needs some TLC: create on TopFieldCollector changed
        return new TopDocSubCollector<TopFieldSuperCollector>(TopFieldCollector.create(this.sort,
                this.numHits, this.numHits), this);
    }

    @Override
    public void complete() {
    }

}

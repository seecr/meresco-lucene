/* begin license *
 *
 * "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
 *
 * Copyright (C) 2015-2016 Koninklijke Bibliotheek (KB) http://www.kb.nl
 * Copyright (C) 2015-2016 Seecr (Seek You Too B.V.) https://seecr.nl
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


public final class MerescoCluster {
    public final MerescoCluster.DocScore[] topDocs;
    public final MerescoCluster.TermScore[] topTerms;

    public MerescoCluster(MerescoCluster.DocScore[] topDocs, MerescoCluster.TermScore[] topTerms) {
        this.topDocs = topDocs;
        this.topTerms = topTerms;
    }

    public static final class DocScore {
        public final int docId;
        public final double score;
        public String identifier;

        public DocScore(int docId, double score) {
            this.docId = docId;
            this.score = score;
        }
    }

    public static final class TermScore {
        public final String term;
        public final double score;

        public TermScore(String term, double score) {
            this.term = term;
            this.score = score;
        }
    }
}
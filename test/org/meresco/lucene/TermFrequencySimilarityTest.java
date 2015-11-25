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

package org.meresco.lucene;

import static org.junit.Assert.assertEquals;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.junit.Test;
import org.meresco.lucene.Lucene;
import org.meresco.lucene.LuceneResponse;
import org.meresco.lucene.LuceneSettings;
import org.meresco.lucene.search.TermFrequencySimilarity;

public class TermFrequencySimilarityTest extends SeecrTestCase {

    @Test
    public void test() throws Exception {
        LuceneSettings settings = new LuceneSettings();
        settings.commitCount = 1;
        settings.similarity = new TermFrequencySimilarity();
        Lucene lucene = new Lucene(this.tmpDir, settings);
        Document document = new Document();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 100; i++) {
            sb.append("x ");
        }
        document.add(new TextField("field", sb.toString(), Store.NO));
        lucene.addDocument("identifier", document);

        Query q = new TermQuery(new Term("field", "x"));
        LuceneResponse result = lucene.executeQuery(q);
        assertEquals(0.1, result.hits.get(0).score, 0.0002);

        q.setBoost((float) 10.0);
        result = lucene.executeQuery(q);
        assertEquals(1, result.hits.get(0).score, 0.0002);
    }

}

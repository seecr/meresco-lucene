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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreDoc;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.meresco.lucene.Lucene;
import org.meresco.lucene.LuceneSettings;
import org.meresco.lucene.search.GroupSuperCollector;
import org.meresco.lucene.search.TopScoreDocSuperCollector;

public class GroupCollectorTest extends SeecrTestCase {

    private Lucene lucene;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        LuceneSettings settings = new LuceneSettings();
        settings.commitCount = 1;
        lucene = new Lucene(this.tmpDir, settings);
    }

    @After
    public void tearDown() throws Exception {
        lucene.close();
        super.tearDown();
    }

    @Test
    public void test() throws Exception {
        addDocument("id:0", 42L);
        addDocument("id:1", 42L);
        addDocument("id:2", 42L);
        addDocument("id:3", 17L);
        TopScoreDocSuperCollector tc = new TopScoreDocSuperCollector(100, true);
        GroupSuperCollector c = new GroupSuperCollector("__isformatof__", tc);
        lucene.search(new MatchAllDocsQuery(), null, c);
        assertEquals(4, tc.getTotalHits());
        Map<String, Integer> idFields = new HashMap<String, Integer>();
        for (ScoreDoc scoreDoc : tc.topDocs(0).scoreDocs) {
            idFields.put(lucene.getDocument(scoreDoc.doc).get(Lucene.ID_FIELD), scoreDoc.doc);
        }

        assertEquals(3, c.group(idFields.get("id:0")).size());
        assertEquals(3, c.group(idFields.get("id:1")).size());
        assertEquals(3, c.group(idFields.get("id:2")).size());
        assertEquals(1, c.group(idFields.get("id:3")).size());
    }

    public void addDocument(String identifier, Long isformatof) throws IOException {
        Document doc = new Document();
        if (isformatof != null)
            doc.add(new NumericDocValuesField("__isformatof__", isformatof));
        lucene.addDocument(identifier, doc);
        lucene.commit();
    }
}

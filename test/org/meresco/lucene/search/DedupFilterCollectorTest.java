/* begin license *
 *
 * "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
 *
 * Copyright (C) 2015-2016 Koninklijke Bibliotheek (KB) http://www.kb.nl
 * Copyright (C) 2015-2016 Seecr (Seek You Too B.V.) http://seecr.nl
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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.TopDocs;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.meresco.lucene.Lucene;
import org.meresco.lucene.LuceneSettings;
import org.meresco.lucene.SeecrTestCase;

public class DedupFilterCollectorTest extends SeecrTestCase {
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
    public void testCollectorTransparentlyDelegatesToNextCollector() throws Throwable {
        addDocument("urn:1", 2L, null);
        TopScoreDocSuperCollector tc = new TopScoreDocSuperCollector(100);
        DeDupFilterSuperCollector c = new DeDupFilterSuperCollector("__isformatof__", Arrays.asList("__sort__").toArray(new String[0]), tc);
        lucene.search(new MatchAllDocsQuery(), null, c);
        assertEquals(1, tc.topDocs(0).totalHits);
    }

    public void addDocument(String identifier, Long isformatof, Long sort) throws Throwable {
        Document doc = new Document();
        if (isformatof != null)
            doc.add(new NumericDocValuesField("__isformatof__", isformatof));
        if (sort != null)
            doc.add(new NumericDocValuesField("__sort__", sort));
        lucene.addDocument(identifier, doc);
        lucene.maybeCommitAfterUpdate();  // Explicitly, not required: since commitCount=1.
    }

    @Test
    public void testCollectorFiltersTwoSimilar() throws Throwable {
        addDocument("urn:1", 2L, 1L);
        addDocument("urn:2", 2L, 2L);
        TopScoreDocSuperCollector tc = new TopScoreDocSuperCollector(100);
        DeDupFilterSuperCollector c = new DeDupFilterSuperCollector("__isformatof__", Arrays.asList("__sort__").toArray(new String[0]), tc);
        lucene.search(new MatchAllDocsQuery(), null, c);
        TopDocs topDocsResult = tc.topDocs(0);
        assertEquals(1, topDocsResult.totalHits);
        assertEquals(1, topDocsResult.scoreDocs.length);

        int docId = topDocsResult.scoreDocs[0].doc;
        DeDupFilterSuperCollector.Key key = c.keyForDocId(docId);
        String identifier = lucene.getDocument(key.getDocId()).get(Lucene.ID_FIELD);
        assertEquals("urn:2", identifier);
        assertEquals(2, key.getCount());
    }

    @Test
    public void testCollectorFiltersTwoTimesTwoSimilarOneNot() throws Throwable {
        addDocument("urn:1",  1L, 2001L);
        addDocument("urn:2",  3L, 2009L); // result 2x
        addDocument("urn:3", 50L, 2010L); // result 1x
        addDocument("urn:4",  3L, 2001L);
        addDocument("urn:5",  1L, 2009L); // result 2x
        //expected: "urn:2', "urn:3" and "urn:5" in no particular order
        TopScoreDocSuperCollector tc = new TopScoreDocSuperCollector(100);
        DeDupFilterSuperCollector c = new DeDupFilterSuperCollector("__isformatof__", Arrays.asList("__sort__").toArray(new String[0]), tc);
        lucene.search(new MatchAllDocsQuery(), null, c);
        TopDocs topDocsResult = tc.topDocs(0);
        assertEquals(3, topDocsResult.totalHits);
        int size = topDocsResult.scoreDocs.length;
        assertEquals(3, size);
        int[] netDocIds = new int[size];
        String[] identifiers = new String[size];
        int[] counts = new int[size];
        for (int i = 0; i < size; i++) {
            netDocIds[i] = c.keyForDocId(topDocsResult.scoreDocs[i].doc).getDocId();
            identifiers[i] = lucene.getDocument(netDocIds[i]).get(Lucene.ID_FIELD);
            counts[i] = c.keyForDocId(netDocIds[i]).getCount();
        }
        Arrays.sort(identifiers);
        Arrays.sort(counts);
        assertArrayEquals(new String[] {"urn:2", "urn:3", "urn:5"}, identifiers);
        assertArrayEquals(new int[] {1,2,2}, counts);
    }

    @Test
    public void testSilentyYieldsWrongResultWhenFieldNameDoesNotMatch() throws Throwable {
        addDocument("urn:1", 2L, null);
        TopScoreDocSuperCollector tc = new TopScoreDocSuperCollector(100);
        DeDupFilterSuperCollector c = new DeDupFilterSuperCollector("__wrong_field__", Arrays.asList("__sort__").toArray(new String[0]), tc);
        lucene.search(new MatchAllDocsQuery(), null, c);
        assertEquals(1, tc.topDocs(0).totalHits);
    }

    @Test
    public void testShouldAddResultsWithoutIsFormatOf() throws Throwable {
        addDocument("urn:1", 2L, null);
        addDocument("urn:2", null, null);
        addDocument("urn:3", 2L, null);
        addDocument("urn:4", null, null);
        addDocument("urn:5", null, null);
        addDocument("urn:6", null, null);
        addDocument("urn:7", null, null);
        addDocument("urn:8", null, null);
        addDocument("urn:9", null, null);
        addDocument("urn:A", null, null);
        addDocument("urn:B", null, null); // trigger a merge;
        TopScoreDocSuperCollector tc = new TopScoreDocSuperCollector(100);
        DeDupFilterSuperCollector c = new DeDupFilterSuperCollector("__isformatof__", Arrays.asList("__sort__").toArray(new String[0]), tc);
        lucene.search(new MatchAllDocsQuery(), null, c);
        assertEquals(10, tc.topDocs(0).totalHits);
    }
}

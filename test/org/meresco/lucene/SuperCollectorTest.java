/* begin license *
 *
 * "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
 *
 * Copyright (C) 2015 Koninklijke Bibliotheek (KB) http://www.kb.nl
 * Copyright (C) 2015-2016 Seecr (Seek You Too B.V.) http://seecr.nl
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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.facet.FacetField;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.LabelAndValue;
import org.apache.lucene.facet.taxonomy.DocValuesOrdinalsReader;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.BytesRef;
import org.junit.Test;
import org.meresco.lucene.search.FacetSuperCollector;
import org.meresco.lucene.search.MultiSuperCollector;
import org.meresco.lucene.search.SuperCollector;
import org.meresco.lucene.search.TopFieldSuperCollector;
import org.meresco.lucene.search.TopScoreDocSuperCollector;
import org.meresco.lucene.search.TotalHitCountSuperCollector;
import org.meresco.lucene.search.join.ScoreSuperCollector;

public class SuperCollectorTest extends SeecrTestCase {

    @Test
    public void testSearch() throws Throwable {
        TotalHitCountSuperCollector C = new TotalHitCountSuperCollector();
        Lucene I = new Lucene(this.tmpDir, new LuceneSettings());
        MatchAllDocsQuery Q = new MatchAllDocsQuery();
        I.search(Q, null, C);
        assertEquals(0, C.getTotalHits());
        I.addDocument("id1", document("one", "2"));
        I.close();
        I = new Lucene(this.tmpDir, new LuceneSettings());
        I.search(Q, null, C);
        assertEquals(1, C.getTotalHits());
    }

    @Test
    public void testSearchTopDocs() throws Throwable {
        Lucene I = new Lucene(this.tmpDir, new LuceneSettings());
        I.addDocument("id1", document("one", "aap noot mies"));
        I.addDocument("id2", document("two", "aap vuur boom"));
        I.addDocument("id3", document("three", "noot boom mies"));
        I.close();
        I = new Lucene(this.tmpDir, new LuceneSettings());
        TopScoreDocSuperCollector C = new TopScoreDocSuperCollector(2);
        MatchAllDocsQuery Q = new MatchAllDocsQuery();
        I.search(Q, null, C);
        TopDocs td = C.topDocs(0);
        assertEquals(3, C.getTotalHits());
        assertEquals(3, td.totalHits);
        assertEquals(2, td.scoreDocs.length);
    }

    @Test
    public void testSearchTopDocsWithStart() throws Throwable {
        Lucene I = new Lucene(this.tmpDir, new LuceneSettings());
        I.addDocument("id1", document("one", "aap noot mies"));
        I.addDocument("id2", document("two", "aap vuur boom"));
        I.addDocument("id3", document("three", "noot boom mies"));
        I.close();
        I = new Lucene(this.tmpDir, new LuceneSettings());
        TopScoreDocSuperCollector C = new TopScoreDocSuperCollector(2);
        MatchAllDocsQuery Q = new MatchAllDocsQuery();
        I.search(Q, null, C);
        TopDocs td = C.topDocs(1);
        assertEquals(3, C.getTotalHits());
        assertEquals(3, td.totalHits);
        assertEquals(1, td.scoreDocs.length);
        assertEquals(1, td.scoreDocs[0].score, 0);
    }

    @Test
    public void testFacetSuperCollector() throws Throwable {
        Lucene I = new Lucene(this.tmpDir, new LuceneSettings());
        for (int i = 0; i < 1000; i++) {
            Map<String, String> fields = new HashMap<String, String>();
            fields.put("field1", Integer.toString(i));
            fields.put("field2", new String(new char[1000]).replace("\0", Integer.toString(i)));
            Map<String, String> facets = new HashMap<String, String>();
            facets.put("facet1", "value" + (i % 100));
            Document document1 = createDocument(fields,facets);
            I.addDocument("id" + i, document1);
        }
        I.close();
        I = new Lucene(this.tmpDir, new LuceneSettings());
        FacetSuperCollector C = new FacetSuperCollector(I.data.getManager().acquire().taxonomyReader, I.data.getFacetsConfig(), new DocValuesOrdinalsReader());
        MatchAllDocsQuery Q = new MatchAllDocsQuery();
        I.search(Q, null, C);
        FacetResult tc = C.getTopChildren(10, "facet1");
        LabelAndValue[] expected = new LabelAndValue[] {
                new LabelAndValue("value90", 10),
                new LabelAndValue("value91", 10),
                new LabelAndValue("value92", 10),
                new LabelAndValue("value93", 10),
                new LabelAndValue("value94", 10),
                new LabelAndValue("value95", 10),
                new LabelAndValue("value96", 10),
                new LabelAndValue("value97", 10),
                new LabelAndValue("value98", 10),
                new LabelAndValue("value99", 10)
        };
        assertArrayEquals(expected, tc.labelValues);
    }

    @Test
    public void testFacetAndTopsMultiCollector() throws Throwable {
        Lucene I = new Lucene(this.tmpDir, new LuceneSettings());
        for (int i = 0; i < 99; i++) {
            Map<String, String> fields = new HashMap<String, String>();
            fields.put("field1", Integer.toString(i));
            fields.put("field2", new String(new char[1000]).replace("\0", Integer.toString(i)));
            Map<String, String> facets = new HashMap<String, String>();
            facets.put("facet1", "value" + (i % 10));
            Document document1 = createDocument(fields,facets);
            I.addDocument("id" + i, document1);
        }
        I.maybeCommitAfterUpdate();
        I.close();
        I = new Lucene(this.tmpDir, new LuceneSettings());

        FacetSuperCollector f = new FacetSuperCollector(I.data.getManager().acquire().taxonomyReader, I.data.getFacetsConfig(), new DocValuesOrdinalsReader());
        TopScoreDocSuperCollector t = new TopScoreDocSuperCollector(10);
        List<SuperCollector<?>> collectors = new ArrayList<SuperCollector<?>>();
        collectors.add(t);
        collectors.add(f);
        MultiSuperCollector C = new MultiSuperCollector(collectors);
        MatchAllDocsQuery Q = new MatchAllDocsQuery();
        I.search(Q, null, C);

        assertEquals(99, t.topDocs(0).totalHits);
        assertEquals(10, t.topDocs(0).scoreDocs.length);
        FacetResult tc = f.getTopChildren(10, "facet1");

        LabelAndValue[] expected = new LabelAndValue[] {
                new LabelAndValue("value0", 10),
                new LabelAndValue("value1", 10),
                new LabelAndValue("value2", 10),
                new LabelAndValue("value3", 10),
                new LabelAndValue("value4", 10),
                new LabelAndValue("value5", 10),
                new LabelAndValue("value6", 10),
                new LabelAndValue("value7", 10),
                new LabelAndValue("value8", 10),
                new LabelAndValue("value9", 9)
        };
        assertArrayEquals(expected, tc.labelValues);
    }

    @Test
    public void testSearchTopField() throws Throwable {
        Lucene I = new Lucene(this.tmpDir, new LuceneSettings());
        I.addDocument("id1", document("one", "aap noot mies"));
        I.maybeCommitAfterUpdate();
        I.addDocument("id2", document("two", "aap vuur boom"));
        I.maybeCommitAfterUpdate();
        I.addDocument("id3", document("three", "noot boom mies"));
        I.maybeCommitAfterUpdate();
        I.close();
        I = new Lucene(this.tmpDir, new LuceneSettings());
        Sort sort = new Sort(new SortField("name", SortField.Type.STRING, true));
        TopFieldSuperCollector C = new TopFieldSuperCollector(sort, 2, true, false);
        MatchAllDocsQuery Q = new MatchAllDocsQuery();
        I.search(Q, null, C);
        TopDocs td = C.topDocs(0);
        assertEquals(3, C.getTotalHits());
        assertEquals(3, td.totalHits);
        assertEquals(2, td.scoreDocs.length);
        assertEquals("id2", I.getDocument(td.scoreDocs[0].doc).get("__id__"));
        assertEquals("id3", I.getDocument(td.scoreDocs[1].doc).get("__id__"));
    }
    
    @Test
    public void testScoreCollector() throws Throwable {
        Lucene I = new Lucene(this.tmpDir, new LuceneSettings());
        I.addDocument("id1", document("one", "aap noot mies"));
        I.maybeCommitAfterUpdate();
        I.addDocument("id2", document("two", "aap vuur boom"));
        I.maybeCommitAfterUpdate();
        I.addDocument("id3", document("three", "noot boom mies"));
        I.maybeCommitAfterUpdate();
        I.close();
        I = new Lucene(this.tmpDir, new LuceneSettings());
        ScoreSuperCollector C = new ScoreSuperCollector("key");
        MatchAllDocsQuery Q = new MatchAllDocsQuery();
        I.search(Q, null, C);
        assertEquals(1, C.score(1), 0);
        assertEquals(0, C.score(2), 0);
    }

    private Document document(String name, String price) {
        Document doc = new Document();
        doc.add(new StringField("name", name, Store.NO));
        doc.add(new StringField("price", price, Store.NO));
        doc.add(new SortedDocValuesField("name", new BytesRef(name)));
        doc.add(new NumericDocValuesField("key", 1));
        return doc;
    }

    private Document createDocument(Map<String, String> fields, Map<String, String> facets) {
        Document doc = new Document();
        for(String x : fields.keySet()) {
            doc.add(new StringField(x, fields.get(x), Store.NO));
        }

        for(String x : facets.keySet()) {
            doc.add(new FacetField(x, facets.get(x)));
        }
        return doc;
    }
}

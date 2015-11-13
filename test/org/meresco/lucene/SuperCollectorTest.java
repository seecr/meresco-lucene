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
import java.util.Iterator;
import java.util.Map;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.facet.FacetField;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.taxonomy.DocValuesOrdinalsReader;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.junit.Test;
import org.meresco.lucene.Lucene;
import org.meresco.lucene.LuceneSettings;
import org.meresco.lucene.search.FacetSuperCollector;
import org.meresco.lucene.search.MultiSuperCollector;
import org.meresco.lucene.search.TopFieldSuperCollector;
import org.meresco.lucene.search.TopScoreDocSuperCollector;
import org.meresco.lucene.search.TotalHitCountSuperCollector;

public class SuperCollectorTest extends SeecrTestCase {

    @Test
    public void testSearch() throws Exception {
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
    public void testSearchTopDocs() throws Exception {
        Lucene I = new Lucene(this.tmpDir, new LuceneSettings());
        I.addDocument("id1", document("one", "aap noot mies"));
        I.addDocument("id2", document("two", "aap vuur boom"));
        I.addDocument("id3", document("three", "noot boom mies"));
        I.close();
        I = new Lucene(this.tmpDir, new LuceneSettings());
        TopScoreDocSuperCollector C = new TopScoreDocSuperCollector(2, true);
        MatchAllDocsQuery Q = new MatchAllDocsQuery();
        I.search(Q, null, C);
        TopDocs td = C.topDocs(0);
        assertEquals(3, C.getTotalHits());
        assertEquals(3, td.totalHits);
        assertEquals(2, td.scoreDocs.length);
    }

    @Test
    public void testSearchTopDocsWithStart() throws Exception {
        Lucene I = new Lucene(this.tmpDir, new LuceneSettings());
        I.addDocument("id1", document("one", "aap noot mies"));
        I.addDocument("id2", document("two", "aap vuur boom"));
        I.addDocument("id3", document("three", "noot boom mies"));
        I.close();
        I = new Lucene(this.tmpDir, new LuceneSettings());
        TopScoreDocSuperCollector C = new TopScoreDocSuperCollector(2, true);
        MatchAllDocsQuery Q = new MatchAllDocsQuery();
        I.search(Q, null, C);
        TopDocs td = C.topDocs(1);
        assertEquals(3, C.getTotalHits());
        assertEquals(3, td.totalHits);
        assertEquals(1, td.scoreDocs.length);
        assertEquals(1, td.scoreDocs[0].score, 0);
    }

    @Test
    public void testFacetSuperCollector() throws Exception {
        Lucene I = new Lucene(this.tmpDir, new LuceneSettings());
        for (int i = 0; i < 1000; i++) {
            Map<String, String> fields = new HashMap<String, String>();
            fields.put("field1", Integer.toString(i));
            fields.put("field2", new String(new char[1000]).replace("\0", Integer.toString(i)));
            Map<String, String> facets = new HashMap<String, String>();
            facets.put("facet1", "value" + i % 100);
            Document document1 = createDocument(fields,facets);
            document1 = I.facetsConfig.build(I.taxoWriter, document1);
            I.addDocument("id1", document1);
        }
        I.close();
        I = new Lucene(this.tmpDir, new LuceneSettings());
        FacetSuperCollector C = new FacetSuperCollector(I.indexAndTaxo.taxoReader, I.facetsConfig, new DocValuesOrdinalsReader());
        MatchAllDocsQuery Q = new MatchAllDocsQuery();
        I.search(Q, null, C);
        FacetResult tc = C.getTopChildren(10, "facet1", new String[0]);
//        assertEquals([
//                ('value90', 10),
//                ('value91', 10),
//                ('value92', 10),
//                ('value93', 10),
//                ('value94', 10),
//                ('value95', 10),
//                ('value96', 10),
//                ('value97', 10),
//                ('value98', 10),
//                ('value99', 10)
//            ], [(l.label, l.value.intValue()) for l in tc.labelValues]);
    }
//
//    @Test
//    public void testFacetAndTopsMultiCollector() {
//        I = new Lucene(this.tmpDir, new LuceneSettings());
//        for i in xrange(99):
//            document1 = createDocument(fields=[("field1", str(i)), ("field2", str(i)*1000)], facets=[("facet1", "value%s" % (i % 10))]);
//            document1 = I._facetsConfig.build(I._taxoWriter, document1);
//            I.addDocument("id1", document1);
//        I.commit();
//        I.close();
//        I = new Lucene(this.tmpDir, new LuceneSettings());
//
//        f = new FacetSuperCollector(I._indexAndTaxonomy.taxoReader, I._facetsConfig, new DocValuesOrdinalsReader());
//        t = new TopScoreDocSuperCollector(10, true);
//        collectors = ArrayList().of_(SuperCollector);
//        collectors.add(t);
//        collectors.add(f);
//        C = new MultiSuperCollector(collectors);
//        Q = new MatchAllDocsQuery();
//        I.search(Q, null, C);
//
//        assertEquals(99, t.topDocs(0).totalHits);
//        assertEquals(10, len(t.topDocs(0).scoreDocs));
//        tc = f.getTopChildren(10, "facet1", []);
//
//        assertEquals([
//                ('value0', 10),
//                ('value1', 10),
//                ('value2', 10),
//                ('value3', 10),
//                ('value4', 10),
//                ('value5', 10),
//                ('value6', 10),
//                ('value7', 10),
//                ('value8', 10),
//                ('value9', 9)
//            ], [(l.label, l.value.intValue()) for l in tc.labelValues]);
//    }
//
//    @Test
//    public void testSearchTopField() {
//        I = new Lucene(this.tmpDir, new LuceneSettings());
//        I.addDocument("id1", document(__id__='1', name="one", price="aap noot mies"));
//        I.commit();
//        I.addDocument("id1", document(__id__='2', name="two", price="aap vuur boom"));
//        I.commit();
//        I.addDocument("id1", document(__id__='3', name="three", price="noot boom mies"));
//        I.commit();
//        I.close();
//        I = new Lucene(this.tmpDir, new LuceneSettings());
//        sort = new Sort(new SortField("name", SortField.Type.STRING, true));
//        C = new TopFieldSuperCollector(sort, 2, true, false, true);
//        Q = new MatchAllDocsQuery();
//        I.search(Q, null, C);
//        td = C.topDocs(0);
//        assertEquals(3, C.getTotalHits());
//        assertEquals(3, td.totalHits);
//        assertEquals(2, len(td.scoreDocs));
//        assertEquals(['2', '3'], [I.getDocument(s.doc).get("__id__") for s in td.scoreDocs]);
//    }

    private Document document(String name, String price) {
        Document doc = new Document();
        doc.add(new StringField("name", name, Store.NO));
        doc.add(new StringField("price", name, Store.NO));
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

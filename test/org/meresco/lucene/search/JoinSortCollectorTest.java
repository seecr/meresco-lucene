/* begin license *
 *
 * "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
 *
 * Copyright (C) 2016 Koninklijke Bibliotheek (KB) http://www.kb.nl
 * Copyright (C) 2016 Seecr (Seek You Too B.V.) https://seecr.nl
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

import static org.junit.Assert.assertEquals;

import java.util.Arrays;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.meresco.lucene.ComposedQuery;
import org.meresco.lucene.Lucene;
import org.meresco.lucene.LuceneResponse;
import org.meresco.lucene.LuceneSettings;
import org.meresco.lucene.MultiLucene;
import org.meresco.lucene.SeecrTestCase;


public class JoinSortCollectorTest extends SeecrTestCase {
    private Lucene luceneA;
    private Lucene luceneB;
    private MultiLucene multiLucene;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        LuceneSettings settings = new LuceneSettings();
        settings.commitCount = 1;
        luceneA = new Lucene("A", this.tmpDir.resolve("a"));
        luceneA.initSettings(settings);
        luceneB = new Lucene("B", this.tmpDir.resolve("b"));
        luceneB.initSettings(settings);
        multiLucene = new MultiLucene(Arrays.asList(luceneA, luceneB));

        ////// BBBBBBBBBBBBBBBB ///////
        Document doc1 = new Document();
        doc1.add(new SortedDocValuesField("sortfieldB", new BytesRef("f")));
        doc1.add(new NumericDocValuesField("intSortfieldB", 11));
        doc1.add(new NumericDocValuesField("doubleSortfieldB", NumericUtils.doubleToSortableLong(1.0)));
        doc1.add(new NumericDocValuesField("keyB", 7));
        luceneB.addDocument(doc1);

        Document doc2 = new Document();
        doc2.add(new SortedDocValuesField("sortfieldB", new BytesRef("e")));
        doc2.add(new NumericDocValuesField("intSortfieldB", 12));
        doc2.add(new NumericDocValuesField("doubleSortfieldB", NumericUtils.doubleToSortableLong(2.0)));
        doc2.add(new NumericDocValuesField("keyB", 8));
        luceneB.addDocument(doc2);

        Document doc5 = new Document();
        doc5.add(new SortedDocValuesField("sortfieldB", new BytesRef("d")));
        doc5.add(new NumericDocValuesField("intSortfieldB", 13));
        doc5.add(new NumericDocValuesField("doubleSortfieldB", NumericUtils.doubleToSortableLong(3.0)));
        luceneB.addDocument(doc5);

        ////// AAAAAAAAAAAAAAAA //////
        Document doc3 = new Document();
        doc3.add(new NumericDocValuesField("keyA", 7));
        doc3.add(new SortedDocValuesField("sortfieldA", new BytesRef("a")));
        doc3.add(new NumericDocValuesField("intSortfieldA", 21));
        doc3.add(new NumericDocValuesField("doubleSortfieldA", NumericUtils.doubleToSortableLong(1.0)));
        luceneA.addDocument("id3", doc3);

        Document doc4 = new Document();
        doc4.add(new NumericDocValuesField("keyA", 8));
        doc4.add(new SortedDocValuesField("sortfieldA", new BytesRef("b")));
        doc4.add(new NumericDocValuesField("intSortfieldA", 22));
        doc4.add(new NumericDocValuesField("doubleSortfieldA", NumericUtils.doubleToSortableLong(2.0)));
        luceneA.addDocument("id4", doc4);
        
        Document doc6 = new Document();
        doc6.add(new SortedDocValuesField("sortfieldA", new BytesRef("c")));
        doc6.add(new NumericDocValuesField("intSortfieldA", 23));
        doc6.add(new NumericDocValuesField("doubleSortfieldA", NumericUtils.doubleToSortableLong(3.0)));
        luceneA.addDocument("id6", doc6);
    }

    @After
    public void tearDown() throws Exception {
        luceneA.close();
        luceneB.close();
        super.tearDown();
    }

    @Test
    public void testJoinSortStringFieldInDefaultCoreIgnores() throws Throwable {
        ComposedQuery q = new ComposedQuery("A");
        q.addMatch("A", "B", "keyA", "keyB");
        q.setCoreQuery("A", new MatchAllDocsQuery());
        q.queryData.sort = new Sort(new JoinSortField("sortfieldA", SortField.Type.STRING, false, "A"));
        LuceneResponse response = multiLucene.executeComposedQuery(q);
        /* NB!
         * This sorts id6 correctly although id6 has no key in field 'keyA'
         * It works because the missing value is sorted last.
         * (It DOES have 'c' in sortfieldA, but that isn't taken into acount because 
         * we search via a match on keyA, and the key isn't there for id6.)
         */
        assertEquals(3, response.total);
        assertEquals("id3", response.hits.get(0).id);
        assertEquals("id4", response.hits.get(1).id);
        assertEquals("id6", response.hits.get(2).id);

        q.queryData.sort = new Sort(new JoinSortField("sortfieldA", SortField.Type.STRING, true, "A"));
        response = multiLucene.executeComposedQuery(q);
        assertEquals(3, response.total);
        assertEquals("id6", response.hits.get(0).id);
        assertEquals("id4", response.hits.get(1).id);
        assertEquals("id3", response.hits.get(2).id);
    }

    @Test
    public void testJoinSortStringField() throws Throwable {
        ComposedQuery q = new ComposedQuery("A");
        q.addMatch("A", "B", "keyA", "keyB");
        q.setCoreQuery("A", new MatchAllDocsQuery());
        q.queryData.sort = new Sort(new JoinSortField("sortfieldB", SortField.Type.STRING, false, "B"));
        LuceneResponse response = multiLucene.executeComposedQuery(q);
        assertEquals(3, response.total);
        assertEquals("id4", response.hits.get(0).id);
        assertEquals("id3", response.hits.get(1).id);
        assertEquals("id6", response.hits.get(2).id);

        q.queryData.sort = new Sort(new JoinSortField("sortfieldB", SortField.Type.STRING, true, "B"));
        response = multiLucene.executeComposedQuery(q);
        assertEquals(3, response.total);
        assertEquals("id6", response.hits.get(0).id);
        assertEquals("id3", response.hits.get(1).id);
        assertEquals("id4", response.hits.get(2).id);
    }

    @Test
    public void testJoinSortIntField() throws Throwable {
        ComposedQuery q = new ComposedQuery("A");
        q.addMatch("A", "B", "keyA", "keyB");
        q.setCoreQuery("A", new MatchAllDocsQuery());
        q.queryData.sort = new Sort(new JoinSortField("intSortfieldB", SortField.Type.INT, false, "B"));
        LuceneResponse response = multiLucene.executeComposedQuery(q);
        assertEquals(3, response.total);
        assertEquals("id6", response.hits.get(0).id);
        assertEquals("id3", response.hits.get(1).id);
        assertEquals("id4", response.hits.get(2).id);

        q.queryData.sort = new Sort(new JoinSortField("intSortfieldB", SortField.Type.INT, true, "B"));
        response = multiLucene.executeComposedQuery(q);
        assertEquals(3, response.total);
        assertEquals("id4", response.hits.get(0).id);
        assertEquals("id3", response.hits.get(1).id);
        assertEquals("id6", response.hits.get(2).id);
    }

    @Test
    public void testJoinSortDoubleField() throws Throwable {
        ComposedQuery q = new ComposedQuery("A");
        q.addMatch("A", "B", "keyA", "keyB");
        q.setCoreQuery("A", new MatchAllDocsQuery());
        q.queryData.sort = new Sort(new JoinSortField("doubleSortfieldB", SortField.Type.DOUBLE, false, "B"));
        LuceneResponse response = multiLucene.executeComposedQuery(q);
        assertEquals(3, response.total);
        assertEquals("id6", response.hits.get(0).id);
        assertEquals("id3", response.hits.get(1).id);
        assertEquals("id4", response.hits.get(2).id);

        q.queryData.sort = new Sort(new JoinSortField("doubleSortfieldB", SortField.Type.DOUBLE, true, "B"));
        response = multiLucene.executeComposedQuery(q);
        assertEquals(3, response.total);
        assertEquals("id4", response.hits.get(0).id);
        assertEquals("id3", response.hits.get(1).id);
        assertEquals("id6", response.hits.get(2).id);
    }
}

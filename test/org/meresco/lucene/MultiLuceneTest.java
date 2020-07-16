/* begin license *
 *
 * "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
 *
 * Copyright (C) 2015-2016 Koninklijke Bibliotheek (KB) http://www.kb.nl
 * Copyright (C) 2015-2016, 2020 Seecr (Seek You Too B.V.) https://seecr.nl
 * Copyright (C) 2016, 2020 Stichting Kennisnet https://www.kennisnet.nl
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
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.FixedBitSet;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.meresco.lucene.JsonQueryConverter.FacetRequest;
import org.meresco.lucene.LuceneResponse.DrilldownData;
import org.meresco.lucene.search.TermFrequencySimilarity;
import org.meresco.lucene.search.join.relational.JoinAndQuery;
import org.meresco.lucene.search.join.relational.RelationalLuceneQuery;
import org.meresco.lucene.search.join.relational.RelationalNotQuery;
import org.meresco.lucene.search.join.relational.WrappedRelationalQuery;


public class MultiLuceneTest extends SeecrTestCase {
    private Lucene luceneA;
    private Lucene luceneB;
    private Lucene luceneC;
    private MultiLucene multiLucene;

    @Override
    @SuppressWarnings({ "unchecked", "serial", "rawtypes" })
    @Before
    public void setUp() throws Exception {
        super.setUp();
        LuceneSettings settingsA = new LuceneSettings();
        LuceneSettings settingsB = new LuceneSettings();
        LuceneSettings settingsC = new LuceneSettings();
        settingsC.similarity = new TermFrequencySimilarity();
        this.luceneA = new Lucene("coreA", this.tmpDir.resolve("a"), settingsA);
        this.luceneB = new Lucene("coreB", this.tmpDir.resolve("b"), settingsB);
        this.luceneC = new Lucene("coreC", this.tmpDir.resolve("c"), settingsC);
        this.multiLucene = new MultiLucene(new ArrayList<Lucene>() {{ this.add(MultiLuceneTest.this.luceneA); this.add(MultiLuceneTest.this.luceneB); this.add(MultiLuceneTest.this.luceneC);}});
        prepareFixture(this.luceneA, this.luceneB, this.luceneC);
    }

    @SuppressWarnings({ "unchecked", "serial", "rawtypes" })
    public
    static void prepareFixture(Lucene luceneA, Lucene luceneB, Lucene luceneC) throws Exception {
        LuceneTest.addDocument(luceneA, "A",      new HashMap() {{this.put("A", 1);}}, new HashMap() {{this.put("M", "false"); this.put("Q", "false"); this.put("U", "false"); this.put("S", "1");}});
        LuceneTest.addDocument(luceneA, "A-U",    new HashMap() {{this.put("A", 2 );}}, new HashMap() {{this.put("M", "false"); this.put("Q", "false"); this.put("U", "true" ); this.put("S", "2");}});
        LuceneTest.addDocument(luceneA, "A-Q",    new HashMap() {{this.put("A", 3 );}}, new HashMap() {{this.put("M", "false"); this.put("Q", "true" ); this.put("U", "false"); this.put("S", "3");}});
        LuceneTest.addDocument(luceneA, "A-QU",   new HashMap() {{this.put("A", 4 );}}, new HashMap() {{this.put("M", "false"); this.put("Q", "true" ); this.put("U", "true" ); this.put("S", "4");}});
        LuceneTest.addDocument(luceneA, "A-M",    new HashMap() {{this.put("A", 5 ); this.put("C", 5);}}, new HashMap() {{this.put("M", "true" ); this.put("Q", "false"); this.put("U", "false"); this.put("S", "5");}});
        LuceneTest.addDocument(luceneA, "A-MU",   new HashMap() {{this.put("A", 6 ); this.put("C", 12);}}, new HashMap() {{this.put("M", "true" ); this.put("Q", "false"); this.put("U", "true" ); this.put("S", "6");}});
        LuceneTest.addDocument(luceneA, "A-MQ",   new HashMap() {{this.put("A", 7 );}}, new HashMap() {{this.put("M", "true" ); this.put("Q", "true" ); this.put("U", "false"); this.put("S", "7");}});
        LuceneTest.addDocument(luceneA, "A-MQU",  new HashMap() {{this.put("A", 8 );}}, new HashMap() {{this.put("M", "true" ); this.put("Q", "true" ); this.put("U", "true" ); this.put("S", "8");}});

        LuceneTest.addDocument(luceneB, "B-N>A-M",   new HashMap() {{this.put("B", 5 ); this.put("D", 5);}}, new HashMap() {{this.put("N", "true" ); this.put("O", "true" ); this.put("P", "false"); this.put("T", "A"); this.put("intField", "1");}});
        LuceneTest.addDocument(luceneB, "B-N>A-MU",  new HashMap() {{this.put("B", 6 );}}, new HashMap() {{this.put("N", "true" ); this.put("O", "false"); this.put("P", "false"); this.put("T", "B"); this.put("intField", "2");}});
        LuceneTest.addDocument(luceneB, "B-N>A-MQ",  new HashMap() {{this.put("B", 7 );}}, new HashMap() {{this.put("N", "true" ); this.put("O", "true" ); this.put("P", "false"); this.put("T", "C"); this.put("intField", "3");}});
        LuceneTest.addDocument(luceneB, "B-N>A-MQU", new HashMap() {{this.put("B", 8 );}}, new HashMap() {{this.put("N", "true" ); this.put("O", "false"); this.put("P", "false"); this.put("T", "D"); this.put("intField", "4");}});
        LuceneTest.addDocument(luceneB, "B-N",       new HashMap() {{this.put("B", 9 );}}, new HashMap() {{this.put("N", "true" ); this.put("O", "true" ); this.put("P", "false"); this.put("T", "E"); this.put("intField", "5");}});
        LuceneTest.addDocument(luceneB, "B",         new HashMap() {{this.put("B", 10);}}, new HashMap() {{this.put("N", "false"); this.put("O", "false"); this.put("P", "false"); this.put("T", "F"); this.put("intField", "6");}});
        LuceneTest.addDocument(luceneB, "B-P>A-M",   new HashMap() {{this.put("B", 5 );}}, new HashMap() {{this.put("N", "false"); this.put("O", "true" ); this.put("P", "true" ); this.put("T", "G"); this.put("intField", "7");}});
        LuceneTest.addDocument(luceneB, "B-P>A-MU",  new HashMap() {{this.put("B", 6 );}}, new HashMap() {{this.put("N", "false"); this.put("O", "false"); this.put("P", "true" ); this.put("T", "H"); this.put("intField", "8");}});
        LuceneTest.addDocument(luceneB, "B-P>A-MQ",  new HashMap() {{this.put("B", 7 );}}, new HashMap() {{this.put("N", "false"); this.put("O", "false" ); this.put("P", "true" ); this.put("T", "I"); this.put("intField", "9");}});
        LuceneTest.addDocument(luceneB, "B-P>A-MQU", new HashMap() {{this.put("B", 8 );}}, new HashMap() {{this.put("N", "false"); this.put("O", "false"); this.put("P", "true" ); this.put("T", "J"); this.put("intField", "10");}});
        LuceneTest.addDocument(luceneB, "B-P",       new HashMap() {{this.put("B", 11);}}, new HashMap() {{this.put("N", "false"); this.put("O", "true" ); this.put("P", "true" ); this.put("T", "K"); this.put("intField", "11");}});

        LuceneTest.addDocument(luceneC, "C-R", new HashMap() {{this.put("C", 5); this.put("C2", 12);}}, new HashMap() {{this.put("R", "true");}});
        LuceneTest.addDocument(luceneC, "C-S", new HashMap() {{this.put("C", 8);}}, new HashMap() {{this.put("S", "true");}});
        LuceneTest.addDocument(luceneC, "C-S2", new HashMap() {{this.put("C", 7);}}, new HashMap() {{this.put("S", "false");}});

        luceneA.getSettings().commitCount = 1;
        luceneB.getSettings().commitCount = 1;
        luceneC.getSettings().commitCount = 1;
        luceneA.commit();
        luceneB.commit();
        luceneC.commit();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        this.luceneA.close();
        this.luceneB.close();
        this.luceneC.close();
        super.tearDown();
    }

    @Test
    public void testQueryOneIndexWithComposedQuery() throws Throwable {
        ComposedQuery q = new ComposedQuery("coreA", new TermQuery(new Term("Q", "true")));
        LuceneResponse result = this.multiLucene.executeComposedQuery(q);
        assertEquals(4, result.total);
        LuceneTest.compareHits(result, "A-Q", "A-QU", "A-MQ", "A-MQU");
    }

    @Test
    public void testQueryOneIndexWithComposedQueryWithFilterQueries() throws Throwable {
        ComposedQuery q = new ComposedQuery("coreA");
        q.addFilterQuery("coreA", new TermQuery(new Term("Q", "true")));
        LuceneResponse result = this.multiLucene.executeComposedQuery(q);
        assertEquals(4, result.total);
        LuceneTest.compareHits(result, "A-Q", "A-QU", "A-MQ", "A-MQU");
    }

    @Test
    public void testJoinQuery() throws Throwable {
        ComposedQuery q = new ComposedQuery("coreA", new MatchAllDocsQuery());
        q.setCoreQuery("coreB", new TermQuery(new Term("N", "true")));
        q.addMatch("coreA", "coreB", "A", "B");
        LuceneResponse result = this.multiLucene.executeComposedQuery(q);
        assertEquals(4, result.total);
        LuceneTest.compareHits(result, "A-M", "A-MU", "A-MQ", "A-MQU");
    }

    //    testMultipleJoinQueriesKeepsCachesWithinMaxSize
    @Test
    public void testJoinQueryWithFilters() throws Throwable {
        ComposedQuery q = new ComposedQuery("coreA");
        q.addFilterQuery("coreB", new TermQuery(new Term("N", "true")));
        q.addMatch("coreA", "coreB", "A", "B");

        LuceneResponse result = this.multiLucene.executeComposedQuery(q);
        assertEquals(4, result.total);
        LuceneTest.compareHits(result, "A-M", "A-MU", "A-MQ", "A-MQU");
    }

    @Test
    public void testJoinQueryWithExcludeFilters() throws Throwable {
        ComposedQuery q = new ComposedQuery("coreA");
        q.addFilterQuery("coreC", new TermQuery(new Term("S", "true")));
        q.addMatch("coreA", "coreC", "A", "C");

        LuceneResponse result = this.multiLucene.executeComposedQuery(q);
        assertEquals(1, result.total);
        LuceneTest.compareHits(result, "A-MQU");


        q = new ComposedQuery("coreA");
        q.addExcludeFilterQuery("coreC", new TermQuery(new Term("S", "true")));
        q.addMatch("coreA", "coreC", "A", "C");

        result = this.multiLucene.executeComposedQuery(q);
        assertEquals(7, result.total);
        LuceneTest.compareHits(result, "A-M", "A-MU", "A-MQ", "A", "A-U", "A-Q", "A-QU");
    }

    @Test
    public void testJoinWithFacetInResultCore() throws Throwable {
        ComposedQuery q = new ComposedQuery("coreA", new TermQuery(new Term("Q", "true")));
        q.setCoreQuery("coreB", new TermQuery(new Term("O", "true")));
        q.addFacet("coreA", new JsonQueryConverter.FacetRequest("cat_M", 10));
        q.addMatch("coreA", "coreB", "A", "B");

        LuceneResponse result = this.multiLucene.executeComposedQuery(q);
        assertEquals(1, result.total);
        assertEquals(1, result.drilldownData.size());
        assertEquals("true", result.drilldownData.get(0).terms.get(0).label);
        assertEquals(1, result.drilldownData.get(0).terms.get(0).count);
    }

    @Test
    public void testJoinFacet() throws Throwable {
        ComposedQuery q = new ComposedQuery("coreA", new TermQuery(new Term("Q", "true")));
        q.addFacet("coreB", new JsonQueryConverter.FacetRequest("cat_N", 10));
        q.addFacet("coreB", new JsonQueryConverter.FacetRequest("cat_O", 10));
        q.addMatch("coreA", "coreB", "A", "B");

        LuceneResponse result = this.multiLucene.executeComposedQuery(q);
        assertEquals(2, result.drilldownData.size());
        assertEquals("cat_N", result.drilldownData.get(0).fieldname);
        assertEquals("true", result.drilldownData.get(0).terms.get(0).label);
        assertEquals("false", result.drilldownData.get(0).terms.get(1).label);
        assertEquals(2, result.drilldownData.get(0).terms.get(0).count);
        assertEquals(2, result.drilldownData.get(0).terms.get(1).count);
        assertEquals("cat_O", result.drilldownData.get(1).fieldname);
        assertEquals("false", result.drilldownData.get(1).terms.get(0).label);
        assertEquals("true", result.drilldownData.get(1).terms.get(1).label);
        assertEquals(3, result.drilldownData.get(1).terms.get(0).count);
        assertEquals(1, result.drilldownData.get(1).terms.get(1).count);
    }

    @Test
    public void testJoinFacetWithDrilldownQueryFilters() throws Throwable {
        ComposedQuery q = new ComposedQuery("coreA");
        q.setCoreQuery("coreA", new TermQuery(new Term("M", "true")));
        q.addFacet("coreB", new JsonQueryConverter.FacetRequest("cat_O", 10));
        q.addDrilldownQuery("coreA", "cat_Q", "true");
        q.addMatch("coreA", "coreB", "A", "B");

        LuceneResponse result = this.multiLucene.executeComposedQuery(q);
        assertEquals(2, result.total);
        assertEquals(1, result.drilldownData.size());
        DrilldownData catO = result.drilldownData.get(0);
        assertEquals("cat_O", catO.fieldname);
        assertEquals(2, catO.terms.size());
        assertEquals("false", catO.terms.get(0).label);
        assertEquals(3, catO.terms.get(0).count);
        assertEquals("true", catO.terms.get(1).label);
        assertEquals(1, catO.terms.get(1).count);
    }

    @Test
    public void testMultipleDrilldownQueries() throws Throwable {
        ComposedQuery q = new ComposedQuery("coreA");
        q.addDrilldownQuery("coreA", "cat_Q", "true");
        q.addDrilldownQuery("coreA", "cat_Q", "false");

        LuceneResponse result = this.multiLucene.executeComposedQuery(q);
        assertEquals(0, result.total);
    }

    @Test
    public void testJoinFacetWithJoinDrilldownQueryFilters() throws Throwable {
        ComposedQuery q = new ComposedQuery("coreA");
        q.setCoreQuery("coreA", new TermQuery(new Term("M", "true")));
        q.addFacet("coreB", new JsonQueryConverter.FacetRequest("cat_O", 10));
        q.addDrilldownQuery("coreB", "cat_O", "true");
        q.addMatch("coreA", "coreB", "A", "B");

        LuceneResponse result = this.multiLucene.executeComposedQuery(q);
        assertEquals(2, result.total);
        assertEquals(1, result.drilldownData.size());
        DrilldownData catO = result.drilldownData.get(0);
        assertEquals("cat_O", catO.fieldname);
        assertEquals(1, catO.terms.size());
        assertEquals("true", catO.terms.get(0).label);
        assertEquals(3, catO.terms.get(0).count);
    }

    @Test
    public void testJoinDrilldownQueryFilters() throws Throwable {
        ComposedQuery q = new ComposedQuery("coreA");
        q.setCoreQuery("coreA", new TermQuery(new Term("M", "true")));
        q.addDrilldownQuery("coreA", "cat_Q", "true");
        q.addMatch("coreA", "coreB", "A", "B");

        LuceneResponse result = this.multiLucene.executeComposedQuery(q);
        assertEquals(2, result.total);
    }

    @Test
    public void testJoinFacetWithFilter() throws Throwable {
        ComposedQuery q = new ComposedQuery("coreA");
        q.setCoreQuery("coreA", new TermQuery(new Term("M", "true")));
        q.addFilterQuery("coreA", new TermQuery(new Term("Q", "true")));
        q.addFacet("coreB", new JsonQueryConverter.FacetRequest("cat_O", 10));
        q.addMatch("coreA", "coreB", "A", "B");

        LuceneResponse result = this.multiLucene.executeComposedQuery(q);
        assertEquals(2, result.total);

        assertEquals(1, result.drilldownData.size());
        DrilldownData catO = result.drilldownData.get(0);
        assertEquals("cat_O", catO.fieldname);
        assertEquals(2, catO.terms.size());
        assertEquals("false", catO.terms.get(0).label);
        assertEquals(3, catO.terms.get(0).count);
        assertEquals("true", catO.terms.get(1).label);
        assertEquals(1, catO.terms.get(1).count);
    }

    @Test
    public void testJoinFacetWillNotFilter() throws Throwable {
        ComposedQuery q = new ComposedQuery("coreA");
        q.addFacet("coreB", new JsonQueryConverter.FacetRequest("cat_N", 10));
        q.addMatch("coreA", "coreB", "A", "B");

        LuceneResponse result = this.multiLucene.executeComposedQuery(q);
        assertEquals(8, result.total);

        assertEquals(1, result.drilldownData.size());
        DrilldownData catN = result.drilldownData.get(0);
        assertEquals("cat_N", catN.fieldname);
        assertEquals(2, catN.terms.size());
        assertEquals("true", catN.terms.get(0).label);
        assertEquals(4, catN.terms.get(0).count);
        assertEquals("false", catN.terms.get(1).label);
        assertEquals(4, catN.terms.get(1).count);
    }

    @Test
    public void testJoinFacetAndQuery() throws Throwable {
        ComposedQuery q = new ComposedQuery("coreA");
        q.setCoreQuery("coreB", new TermQuery(new Term("N", "true")));
        q.addFacet("coreB", new JsonQueryConverter.FacetRequest("cat_N", 10));
        q.addFacet("coreB", new JsonQueryConverter.FacetRequest("cat_O", 10));
        q.addMatch("coreA", "coreB", "A", "B");

        LuceneResponse result = this.multiLucene.executeComposedQuery(q);
        assertEquals(4, result.total);
        LuceneTest.compareHits(result, "A-M", "A-MU", "A-MQ", "A-MQU");

        assertEquals(2, result.drilldownData.size());
        DrilldownData catN = result.drilldownData.get(0);
        assertEquals("cat_N", catN.fieldname);
        assertEquals(1, catN.terms.size());
        assertEquals("true", catN.terms.get(0).label);
        assertEquals(4, catN.terms.get(0).count);

        DrilldownData catO = result.drilldownData.get(1);
        assertEquals("cat_O", catO.fieldname);
        assertEquals(2, catO.terms.size());
        assertEquals("true", catO.terms.get(0).label);
        assertEquals(2, catO.terms.get(0).count);
        assertEquals("false", catO.terms.get(1).label);
        assertEquals(2, catO.terms.get(1).count);
    }

    @Test
    public void testUniteResultFromTwoIndexes() throws Throwable {
        ComposedQuery q = new ComposedQuery("coreA");
        q.setCoreQuery("coreA", new TermQuery(new Term("Q", "true")));
        q.setCoreQuery("coreB", null);
        q.addMatch("coreA", "coreB", "A", "B");
        q.addUnite("coreA", new TermQuery(new Term("U", "true")), "coreB", new TermQuery(new Term("N", "true")));
        LuceneResponse result = this.multiLucene.executeComposedQuery(q);
        assertEquals(3, result.total);
        LuceneTest.compareHits(result, "A-QU", "A-MQ", "A-MQU");
    }

    @Test
    public void testUniteResultFromTwoIndexesCached() throws Throwable {
        ComposedQuery q = new ComposedQuery("coreA");
        q.setCoreQuery("coreA", new TermQuery(new Term("Q", "true")));
        q.setCoreQuery("coreB", null);
        q.addMatch("coreA", "coreB", "A", "B");
        q.addUnite("coreA", new TermQuery(new Term("U", "true")), "coreB", new TermQuery(new Term("N", "true")));
        LuceneResponse resultOne = this.multiLucene.executeComposedQuery(q);

        q = new ComposedQuery("coreA");
        q.setCoreQuery("coreA", new TermQuery(new Term("U", "true")));
        q.addMatch("coreA", "coreB", "A", "B");
        q.addUnite("coreA", new TermQuery(new Term("U", "false")), "coreB", new TermQuery(new Term("N", "true")));
        this.multiLucene.executeComposedQuery(q);

        q = new ComposedQuery("coreA");
        q.setCoreQuery("coreA", new TermQuery(new Term("Q", "true")));
        q.setCoreQuery("coreB", null);
        q.addMatch("coreA", "coreB", "A", "B");
        q.addUnite("coreA", new TermQuery(new Term("U", "true")), "coreB", new TermQuery(new Term("N", "true")));
        LuceneResponse resultAgain = this.multiLucene.executeComposedQuery(q);
        assertEquals(resultOne.total, resultAgain.total);

        for (int i = 0; i < resultOne.hits.size(); i++) {
            assertEquals(resultOne.hits.get(i).id, resultAgain.hits.get(i).id);
        }
    }

    @SuppressWarnings({ "rawtypes", "serial", "unchecked" })
    @Test
    public void testUniteResultFromTwoIndexesCachedAfterUpdate() throws Throwable {
        ComposedQuery q = new ComposedQuery("coreA");
        q.setCoreQuery("coreA", new TermQuery(new Term("Q", "true")));
        q.setCoreQuery("coreB", null);
        q.addMatch("coreA", "coreB", "A", "B");
        q.addUnite("coreA", new TermQuery(new Term("U", "true")), "coreB", new TermQuery(new Term("N", "true")));
        LuceneResponse resultOne = this.multiLucene.executeComposedQuery(q);
        assertEquals(3, resultOne.total);

        LuceneTest.addDocument(this.luceneA, "A-MQU", new HashMap() {{this.put("A", 8);}}, new HashMap() {{this.put("M", "true"); this.put("Q", "false"); this.put("U", "true"); this.put("S", "8");}});
        LuceneResponse resultAgain = this.multiLucene.executeComposedQuery(q);
        assertEquals(2, resultAgain.total);
    }

    @Test
    public void testUniteResultFromTwoIndexes_filterQueries() throws Throwable {
        ComposedQuery q = new ComposedQuery("coreA");
        q.addFilterQuery("coreA", new TermQuery(new Term("Q", "true")));
        q.addMatch("coreA", "coreB", "A", "B");
        q.addUnite("coreA", new TermQuery(new Term("U", "true")), "coreB", new TermQuery(new Term("N", "true")));
        LuceneResponse result = this.multiLucene.executeComposedQuery(q);
        assertEquals(3, result.total);
        LuceneTest.compareHits(result, "A-QU", "A-MQ", "A-MQU");
    }

    @Test
    public void testUniteAndFacets() throws Throwable {
        ComposedQuery q = new ComposedQuery("coreA", new TermQuery(new Term("Q", "true")));
        q.addFacet("coreA", new JsonQueryConverter.FacetRequest("cat_Q", 10));
        q.addFacet("coreA", new JsonQueryConverter.FacetRequest("cat_U", 10));
        q.addFacet("coreB", new JsonQueryConverter.FacetRequest("cat_N", 10));
        q.addFacet("coreB", new JsonQueryConverter.FacetRequest("cat_O", 10));
        q.addUnite("coreA", new TermQuery(new Term("U", "true")), "coreB", new TermQuery(new Term("N", "true")));
        q.addOtherCoreFacetFilter("coreB", new TermQuery(new Term("N", "true")));
        q.addMatch("coreA", "coreB", "A", "B");

        LuceneResponse result = this.multiLucene.executeComposedQuery(q);
        assertEquals(3, result.total);
        LuceneTest.compareHits(result, "A-QU", "A-MQ", "A-MQU");

        assertEquals(4, result.drilldownData.size());
        DrilldownData catQ = result.drilldownData.get(0);
        assertEquals("cat_Q", catQ.fieldname);
        assertEquals(1, catQ.terms.size());
        assertEquals("true", catQ.terms.get(0).label);
        assertEquals(3, catQ.terms.get(0).count);
        DrilldownData catU = result.drilldownData.get(1);
        assertEquals("cat_U", catU.fieldname);
        assertEquals(2, catU.terms.size());
        assertEquals("true", catU.terms.get(0).label);
        assertEquals(2, catU.terms.get(0).count);
        assertEquals("false", catU.terms.get(1).label);
        assertEquals(1, catU.terms.get(1).count);
        DrilldownData catN = result.drilldownData.get(2);
        assertEquals("cat_N", catN.fieldname);
        assertEquals(1, catN.terms.size());
        assertEquals("true", catN.terms.get(0).label);
        assertEquals(2, catN.terms.get(0).count);
        DrilldownData catO = result.drilldownData.get(3);
        assertEquals("cat_O", catO.fieldname);
        assertEquals(2, catO.terms.size());
        assertEquals("true", catO.terms.get(0).label);
        assertEquals(1, catO.terms.get(0).count);
        assertEquals("false", catO.terms.get(1).label);
        assertEquals(1, catO.terms.get(1).count);
    }

    @Test
    public void testUniteAndFacetsWithForeignQuery() throws Throwable {
        ComposedQuery q = new ComposedQuery("coreA");
        q.setCoreQuery("coreB", new TermQuery(new Term("O", "true")));
        q.addFacet("coreB", new JsonQueryConverter.FacetRequest("cat_N", 10));
        q.addFacet("coreB", new JsonQueryConverter.FacetRequest("cat_O", 10));
        q.addUnite("coreA", new TermQuery(new Term("U", "true")), "coreB", new TermQuery(new Term("N", "true")));
        q.addMatch("coreA", "coreB", "A", "B");

        LuceneResponse result = this.multiLucene.executeComposedQuery(q);
        assertEquals(2, result.total);
        LuceneTest.compareHits(result, "A-M", "A-MQ");

        assertEquals(2, result.drilldownData.size());
        DrilldownData catN = result.drilldownData.get(0);
        assertEquals("cat_N", catN.fieldname);
        assertEquals(2, catN.terms.size());
        assertEquals("true", catN.terms.get(0).label);
        assertEquals(2, catN.terms.get(0).count);
        assertEquals("false", catN.terms.get(1).label);
        assertEquals(1, catN.terms.get(1).count);
        DrilldownData catO = result.drilldownData.get(1);
        assertEquals("cat_O", catO.fieldname);
        assertEquals(1, catO.terms.size());
        assertEquals("true", catO.terms.get(0).label);
        assertEquals(3, catO.terms.get(0).count);
    }

    @Test
    public void testUniteAndFacetsWithForeignQueryWithSpecialFacetsQuery() throws Throwable {
        ComposedQuery q = new ComposedQuery("coreA");
        q.setCoreQuery("coreB", new TermQuery(new Term("O", "true")));
        q.addFacet("coreB", new JsonQueryConverter.FacetRequest("cat_N", 10));
        q.addFacet("coreB", new JsonQueryConverter.FacetRequest("cat_O", 10));
        q.addUnite("coreA", new TermQuery(new Term("U", "true")), "coreB", new TermQuery(new Term("N", "true")));
        q.addOtherCoreFacetFilter("coreB", new TermQuery(new Term("N", "true")));
        q.addMatch("coreA", "coreB", "A", "B");

        LuceneResponse result = this.multiLucene.executeComposedQuery(q);
        assertEquals(2, result.total);
        LuceneTest.compareHits(result, "A-M", "A-MQ");

        assertEquals(2, result.drilldownData.size());
        DrilldownData catN = result.drilldownData.get(0);
        assertEquals("cat_N", catN.fieldname);
        assertEquals(1, catN.terms.size());
        assertEquals("true", catN.terms.get(0).label);
        assertEquals(2, catN.terms.get(0).count);
        DrilldownData catO = result.drilldownData.get(1);
        assertEquals("cat_O", catO.fieldname);
        assertEquals(1, catO.terms.size());
        assertEquals("true", catO.terms.get(0).label);
        assertEquals(2, catO.terms.get(0).count);
    }

    @Test
    public void testUniteMakesItTwoCoreQuery() throws Throwable {
        ComposedQuery q = new ComposedQuery("coreA");
        q.setCoreQuery("coreA", new TermQuery(new Term("Q", "true")));
        q.addUnite("coreA", new TermQuery(new Term("U", "true")), "coreB", new TermQuery(new Term("N", "true")));
        q.addMatch("coreA", "coreB", "A", "B");

        LuceneResponse result = this.multiLucene.executeComposedQuery(q);
        assertEquals(3, result.total);
        LuceneTest.compareHits(result, "A-QU", "A-MQ", "A-MQU");
    }

    @Test
    public void testStartStopSortKeys() throws Throwable {
        ComposedQuery q = new ComposedQuery("coreA");
        q.setCoreQuery("coreA", new TermQuery(new Term("Q", "true")));
        q.setCoreQuery("coreB", null);
        q.addMatch("coreA", "coreB", "A", "B");
        q.addUnite("coreA", new TermQuery(new Term("U", "true")), "coreB", new TermQuery(new Term("N", "true")));
        q.queryData.sort = new Sort(new SortField("S", SortField.Type.STRING, false));
        LuceneResponse result = this.multiLucene.executeComposedQuery(q);
        assertEquals(3, result.total);
        LuceneTest.compareHits(result, "A-QU", "A-MQ", "A-MQU");

        q.queryData.sort = new Sort(new SortField("S", SortField.Type.STRING, true));
        q.queryData.stop = 2;
        result = this.multiLucene.executeComposedQuery(q);
        assertEquals(3, result.total);
        LuceneTest.compareHits(result, "A-MQ", "A-MQU");

        q.queryData.start = 1;
        q.queryData.stop = 10;
        result = this.multiLucene.executeComposedQuery(q);
        assertEquals(3, result.total);
        LuceneTest.compareHits(result, "A-MQ", "A-QU");
    }

    @SuppressWarnings({ "rawtypes", "serial", "unchecked" })
    @Test
    public void testCachingCollectorsAfterUpdate() throws Throwable {
        ComposedQuery q = new ComposedQuery("coreA");
        q.setCoreQuery("coreA", new MatchAllDocsQuery());
        q.setCoreQuery("coreB", new TermQuery(new Term("N", "true")));
        q.addMatch("coreA", "coreB", "A", "B");
        LuceneTest.addDocument(this.luceneB, "B-N>A-MQU", new HashMap() {{this.put("B", 8);}}, new HashMap() {{this.put("N", "true"); this.put("O", "false"); this.put("P", "false");}});
        LuceneResponse result = this.multiLucene.executeComposedQuery(q);
        LuceneTest.compareHits(result, "A-M", "A-MU", "A-MQ", "A-MQU");
        result = this.multiLucene.executeComposedQuery(q);
        LuceneTest.compareHits(result, "A-M", "A-MU", "A-MQ", "A-MQU");
        //assertTrue(result.queryTime < 5);
        LuceneTest.addDocument(this.luceneB, "B-N>A-MQU", new HashMap() {{this.put("B", 80);}}, new HashMap() {{this.put("N", "true"); this.put("O", "false"); this.put("P", "false");}});
        result = this.multiLucene.executeComposedQuery(q);
        LuceneTest.compareHits(result, "A-M", "A-MU", "A-MQ");
    }

    @SuppressWarnings({ "rawtypes", "serial", "unchecked" })
    @Test
    public void testCachingCollectorsAfterUpdateInSegmentWithMultipleDocuments() throws Throwable {
        ComposedQuery q = new ComposedQuery("coreA");
        q.setCoreQuery("coreA", new MatchAllDocsQuery());
        q.setCoreQuery("coreB", new TermQuery(new Term("N", "true")));
        q.addMatch("coreA", "coreB", "A", "B");
        LuceneResponse result = this.multiLucene.executeComposedQuery(q);
        LuceneTest.addDocument(this.luceneB, "B-N>A-MQU", new HashMap() {{this.put("B", 8);}}, new HashMap() {{this.put("N", "true"); this.put("O", "false"); this.put("P", "false");}});
        result = this.multiLucene.executeComposedQuery(q);
        LuceneTest.compareHits(result, "A-M", "A-MU", "A-MQ", "A-MQU");
        result = this.multiLucene.executeComposedQuery(q);
        LuceneTest.compareHits(result, "A-M", "A-MU", "A-MQ", "A-MQU");
        assertTrue(result.queryTime < 5);
        LuceneTest.addDocument(this.luceneB, "B-N>A-MU", new HashMap() {{this.put("B", 60);}}, new HashMap() {{this.put("N", "true"); this.put("O", "false"); this.put("P", "false");}});
        result = this.multiLucene.executeComposedQuery(q);
        LuceneTest.compareHits(result, "A-M", "A-MQ", "A-MQU");
        LuceneTest.compareHits(result, "A-M", "A-MQ", "A-MQU");
    }

    @SuppressWarnings({ "rawtypes", "serial", "unchecked" })
    @Test
    public void testCachingCollectorsAfterDelete() throws Throwable {
        ComposedQuery q = new ComposedQuery("coreA");
        q.setCoreQuery("coreA", new MatchAllDocsQuery());
        q.setCoreQuery("coreB", new TermQuery(new Term("N", "true")));
        q.addMatch("coreA", "coreB", "A", "B");
        LuceneTest.addDocument(this.luceneB, "B-N>A-MQU", new HashMap() {{this.put("B", 8);}}, new HashMap() {{this.put("N", "true"); this.put("O", "false"); this.put("P", "false");}});
        LuceneResponse result = this.multiLucene.executeComposedQuery(q);
        LuceneTest.compareHits(result, "A-M", "A-MU", "A-MQ", "A-MQU");
        this.luceneB.deleteDocument("B-N>A-MU");
        result = this.multiLucene.executeComposedQuery(q);
        LuceneTest.compareHits(result, "A-M", "A-MQ", "A-MQU");
    }

    @Test
    public void testJoinQueryOnOptionalKey() throws Throwable {
        ComposedQuery q = new ComposedQuery("coreA");
        q.setCoreQuery("coreB", new TermQuery(new Term("N", "true")));
        q.addMatch("coreA", "coreB", "C", "B");

        LuceneResponse result = this.multiLucene.executeComposedQuery(q);
        assertEquals(1, result.total);
        LuceneTest.compareHits(result, "A-M");
    }

    @Test
    public void testJoinQueryOnOptionalKeyOtherSide() throws Throwable {
        ComposedQuery q = new ComposedQuery("coreA");
        q.setCoreQuery("coreB", new TermQuery(new Term("N", "true")));
        q.addMatch("coreA", "coreB", "A", "D");

        LuceneResponse result = this.multiLucene.executeComposedQuery(q);
        assertEquals(1, result.total);
        LuceneTest.compareHits(result, "A-M");
    }

    @Test
    public void testJoinQueryThreeCores() throws Throwable {
        ComposedQuery q = new ComposedQuery("coreA");
        q.setCoreQuery("coreB", new TermQuery(new Term("N", "true")));
        q.setCoreQuery("coreC", new TermQuery(new Term("R", "true")));
        q.addFacet("coreA", new JsonQueryConverter.FacetRequest("cat_M", 10));
        q.addFacet("coreB", new JsonQueryConverter.FacetRequest("cat_N", 10));
        q.addFacet("coreC", new JsonQueryConverter.FacetRequest("cat_R", 10));
        q.addMatch("coreA", "coreB", "A", "B");
        q.addMatch("coreA", "coreC", "A", "C");

        LuceneResponse result = this.multiLucene.executeComposedQuery(q);
        assertEquals(1, result.total);
        LuceneTest.compareHits(result, "A-M");
        Collections.sort(result.drilldownData, new Comparator<DrilldownData>(){
            @Override
            public int compare(DrilldownData o1, DrilldownData o2) {
                return o1.fieldname.compareTo(o2.fieldname);
            }
        });
        assertEquals(3, result.drilldownData.size());
        assertEquals("cat_M", result.drilldownData.get(0).fieldname);
        assertEquals(1, result.drilldownData.get(0).terms.size());
        assertEquals("true", result.drilldownData.get(0).terms.get(0).label);
        assertEquals(1, result.drilldownData.get(0).terms.get(0).count);
        assertEquals("cat_N", result.drilldownData.get(1).fieldname);
        assertEquals(1, result.drilldownData.get(1).terms.size());
        assertEquals("true", result.drilldownData.get(1).terms.get(0).label);
        assertEquals(1, result.drilldownData.get(1).terms.get(0).count);
        assertEquals("cat_R", result.drilldownData.get(2).fieldname);
        assertEquals(1, result.drilldownData.get(2).terms.size());
        assertEquals("true", result.drilldownData.get(2).terms.get(0).label);
        assertEquals(1, result.drilldownData.get(2).terms.get(0).count);
    }

    @Test
    public void testRankQuery() throws Throwable {
        ComposedQuery q = new ComposedQuery("coreA");
        q.setCoreQuery("coreA", new MatchAllDocsQuery());
        q.setCoreQuery("coreB", new TermQuery(new Term("N", "true")));
        q.setRankQuery("coreC", new TermQuery(new Term("S", "true")));
        q.addMatch("coreA", "coreB", "A", "B");
        q.addMatch("coreA", "coreC", "A", "C");

        LuceneResponse result = this.multiLucene.executeComposedQuery(q);
        assertEquals(4, result.total);
        LuceneTest.compareHitsOrdered(result, "A-MQU", "A-M", "A-MU", "A-MQ");
    }

    @Test
    public void testMultipleRankQuery() throws Throwable {
        ComposedQuery q = new ComposedQuery("coreA");
        q.setCoreQuery("coreA", new MatchAllDocsQuery());
        q.setCoreQuery("coreB", new TermQuery(new Term("N", "true")));
        q.setRankQuery("coreA", new TermQuery(new Term("Q", "true")));
        q.setRankQuery("coreC", new TermQuery(new Term("S", "true")));
        q.addMatch("coreA", "coreB", "A", "B");
        q.addMatch("coreA", "coreC", "A", "C");

        LuceneResponse result = this.multiLucene.executeComposedQuery(q);
        assertEquals(4, result.total);
        LuceneTest.compareHitsOrdered(result, "A-MQU", "A-MQ", "A-M", "A-MU");
    }

    @SuppressWarnings({ "rawtypes", "serial", "unchecked" })
    @Test
    public void testScoreCollectorCacheInvalidation() throws Throwable {
        ComposedQuery q = new ComposedQuery("coreA");
        q.setRankQuery("coreC", new TermQuery(new Term("S", "true")));
        q.addMatch("coreA", "coreC", "A", "C");
        LuceneResponse result = this.multiLucene.executeComposedQuery(q);
        assertEquals(8, result.total);
        assertEquals("A-MQU", result.hits.get(0).id);
        LuceneTest.compareHits(result, "A", "A-U", "A-Q", "A-QU", "A-M", "A-MU", "A-MQ", "A-MQU");

        LuceneTest.addDocument(this.luceneC, "C-S>A-MQ", new HashMap() {{this.put("C", 7);}}, new HashMap() {{this.put("S", "true");}});
        try {
            result = this.multiLucene.executeComposedQuery(q);
            assertEquals(8, result.total);
            assertEquals("A-MQ", result.hits.get(0).id);
            assertEquals("A-MQU", result.hits.get(1).id);
            LuceneTest.compareHits(result, "A", "A-U", "A-Q", "A-QU", "A-M", "A-MU", "A-MQ", "A-MQU");
        } finally {
            this.luceneC.deleteDocument("C-S>A-MQ");
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes", "serial" })
    @Test
    public void testNullIteratorOfPForDeltaIsIgnoredInFinalKeySet() throws Throwable {
        ComposedQuery q = new ComposedQuery("coreA");
        q.setCoreQuery("coreB", new TermQuery(new Term("N", "no_match")));
        q.setCoreQuery("coreB", new TermQuery(new Term("N", "true")));
        q.addMatch("coreA", "coreB", "UNKOWN", "UNKOWN");
        this.multiLucene.executeComposedQuery(q);
        this.luceneB.maybeCommitAfterUpdate(); // Force to write new segment; Old segment remains in seen list
        LuceneTest.addDocument(this.luceneB, "new", null, new HashMap() {{this.put("ignored", "true");}}); // Add new document to force recreating finalKeySet
        try {
            LuceneResponse result = this.multiLucene.executeComposedQuery(q);
            assertEquals(0, result.hits.size());
        } finally {
            this.luceneB.deleteDocument("new");
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes", "serial" })
    @Test
    public void testKeyFilterIgnoresKeysOutOfBoundsOfKeySet() throws Throwable {
        LuceneTest.addDocument(this.luceneB, "100", new HashMap() {{this.put("B", 100);}}, null); // Force key to be much more than bits in long[] in FixedBitSet, so it must be OutOfBounds
        ComposedQuery q = new ComposedQuery("coreA");
        q.setCoreQuery("coreA", new MatchAllDocsQuery());
        q.setCoreQuery("coreB", new MatchAllDocsQuery());
        q.addMatch("coreA", "coreB", "A", "B");
        LuceneResponse result = this.multiLucene.executeComposedQuery(q);
        assertEquals(4, result.hits.size());
    }

    @Test
    public void testCollectScoresWithNoResultAndBooleanQueryDoesntFailOnFakeScorerInAggregateScoreCollector() throws Throwable {
        BooleanQuery.Builder q = new BooleanQuery.Builder();
        q.add(new TermQuery(new Term("M", "true")), Occur.SHOULD);
        q.add(new TermQuery(new Term("M", "true")), Occur.SHOULD);
        ComposedQuery cq = new ComposedQuery("coreA", q.build());
        cq.queryData.start = 0;
        cq.queryData.stop = 0;
        cq.setRankQuery("coreC", new TermQuery(new Term("S", "true")));
        cq.addMatch("coreA", "coreC", "A", "C");
        LuceneResponse result = this.multiLucene.executeComposedQuery(cq);
        assertEquals(4, result.total);
        assertEquals(0, result.hits.size());
    }

    @Test
    public void testCachingKeyCollectorsIntersectsWithACopyOfTheKeys() throws Throwable {
        ComposedQuery q = new ComposedQuery("coreA");
        q.setCoreQuery("coreA", new MatchAllDocsQuery());
        q.setCoreQuery("coreB", new TermQuery(new Term("O", "true")));
        q.addFilterQuery("coreB", new TermQuery(new Term("N", "true")));
        q.addMatch("coreA", "coreB", "A", "B");
        LuceneResponse result = this.multiLucene.executeComposedQuery(q);
        assertEquals(2, result.hits.size());

        q = new ComposedQuery("coreA");
        q.setCoreQuery("coreA", new MatchAllDocsQuery());
        q.setCoreQuery("coreB", new MatchAllDocsQuery());
        q.addFilterQuery("coreB", new TermQuery(new Term("N", "true")));
        q.addMatch("coreA", "coreB", "A", "B");
        result = this.multiLucene.executeComposedQuery(q);
        assertEquals(4, result.hits.size());
    }

    @Test
    public void testTwoCoreQueryWithThirdCoreDrilldownWithOtherCore() throws Throwable {
        ComposedQuery q = new ComposedQuery("coreA");
        q.setCoreQuery("coreA", new MatchAllDocsQuery());
        q.setCoreQuery("coreB", new MatchAllDocsQuery());
        q.addFacet("coreC", new FacetRequest("cat_R", 10));
        q.addMatch("coreA", "coreB", "A", "B");
        q.addMatch("coreA", "coreC", "C", "C2");
        LuceneResponse result = this.multiLucene.executeComposedQuery(q);
        assertEquals(4, result.total);
        LuceneTest.compareHits(result, "A-M", "A-MQ", "A-MU", "A-MQU");
        assertEquals(1, result.drilldownData.size());
        assertEquals("cat_R", result.drilldownData.get(0).fieldname);
        assertEquals(1, result.drilldownData.get(0).terms.size());
        assertEquals("true", result.drilldownData.get(0).terms.get(0).label);
        assertEquals(1, result.drilldownData.get(0).terms.get(0).count);
    }

    @Test
    public void testFilterQueryInTwoDifferentCores() throws Throwable {
        ComposedQuery q = new ComposedQuery("coreA");
        q.setCoreQuery("coreA", new MatchAllDocsQuery());
        q.setCoreQuery("coreB", new MatchAllDocsQuery());
        q.addFilterQuery("coreB", new TermQuery(new Term("N", "true")));
        q.addFilterQuery("coreC", new MatchAllDocsQuery());
        q.addMatch("coreA", "coreB", "A", "B");
        q.addMatch("coreA", "coreC", "C", "C2");
        LuceneResponse result = this.multiLucene.executeComposedQuery(q);
        LuceneTest.compareHits(result, "A-MU");
        assertEquals(1, result.total);
    }

    @Test
    public void testRelationalFilterQuery() throws Throwable {
        ComposedQuery q = new ComposedQuery("coreA");
        q.setCoreQuery("coreA", new MatchAllDocsQuery());
        q.addFilterQuery("coreA", new WrappedRelationalQuery(
            new JoinAndQuery(
                new RelationalLuceneQuery("coreA", "A", new TermQuery(new Term("Q", "true"))),
                new RelationalLuceneQuery("coreB", "B", new TermQuery(new Term("P", "true")))
            )
        ));

        q.addMatch("coreA", "coreB", "A", "B");  // Note: the actual match is part of the RelationalQuery, but we still need to specify at this level that there are more cores involved (and which ones).

        LuceneResponse result = this.multiLucene.multipleCoreQuery(q, "A");
        LuceneTest.compareHits(result, "A-MQ", "A-MQU");
        assertEquals(2, result.total);

        result = this.multiLucene.executeComposedQuery(q);
        LuceneTest.compareHits(result, "A-MQ", "A-MQU");
        assertEquals(2, result.total);
    }

    @Test
    public void testScoreCollectorOnDifferentKeys() throws Throwable {
        ComposedQuery q = new ComposedQuery("coreA");
        q.setCoreQuery("coreA", new MatchAllDocsQuery());
        q.setRankQuery("coreB", new TermQuery(new Term("N", "true")));
        q.setRankQuery("coreC", new TermQuery(new Term("R", "true")));
        q.addMatch("coreA", "coreB", "A", "B");
        q.addMatch("coreA", "coreC", "C", "C2");
        LuceneResponse result = this.multiLucene.executeComposedQuery(q);
        assertEquals(8, result.total);
        assertEquals("A-MU", result.hits.get(0).id);
        assertTrue(result.hits.get(0).score > result.hits.get(1).score);
    }
    //    testJoinSort
    //    testSortWithJoinField


    @Test
    public void testQueryConvertors() throws Throwable {
        this.luceneA.getSettings().facetsConfig.setIndexFieldName("dim1", "otherfield");
        Map<String, JsonQueryConverter> converters = this.multiLucene.getQueryConverters();
        Term drilldownTerm = converters.get("coreA").createDrilldownTerm("dim1");
        assertEquals("otherfield", drilldownTerm.field());

        drilldownTerm = converters.get("coreB").createDrilldownTerm("dim1");
        assertEquals("$facets", drilldownTerm.field());
    }

    @Test
    public void testExportKeys() throws Throwable {
        ComposedQuery q = new ComposedQuery("coreA");
        q.setCoreQuery("coreA", new MatchAllDocsQuery());
        q.setCoreQuery("coreB", new TermQuery(new Term("N", "true")));
        q.addMatch("coreA", "coreB", "A", "B");
        LuceneResponse result = this.multiLucene.executeComposedQuery(q, "A");
        assertEquals(4, result.total);
        FixedBitSet expected = new FixedBitSet(result.keys.length());
        expected.set(5);
        expected.set(6);
        expected.set(7);
        expected.set(8);
        assertEquals(expected, result.keys);
    }

    @Test
    public void testExportKeysSingleCore() throws Throwable {
        ComposedQuery q = new ComposedQuery("coreA");
        q.setCoreQuery("coreA", new TermQuery(new Term("M", "true")));
        LuceneResponse result = this.multiLucene.executeComposedQuery(q, "A");
        assertEquals(4, result.total);
        FixedBitSet expected = new FixedBitSet(result.keys.length());
        expected.set(5);
        expected.set(6);
        expected.set(7);
        expected.set(8);
        assertEquals(expected, result.keys);
    }

    @Test
    public void testRelationalFilter() throws Throwable {
        ComposedQuery q = new ComposedQuery("coreA", new TermQuery(new Term("M", "true")));
        q.addMatch("coreA", "coreB", "A", "B");
        q.relationalFilter = new WrappedRelationalQuery(
            new RelationalNotQuery(
                new JoinAndQuery(
                    new RelationalLuceneQuery("coreA", "A", new TermQuery(new Term("M", "true"))),
                    new RelationalLuceneQuery("coreB", "B", new TermQuery(new Term("O", "true")))
                )
            )
        );  // "A", "A-U", "A-Q", "A-QU", "A-MU", "A-MQU"
        LuceneResponse result = this.multiLucene.executeComposedQuery(q);
        assertEquals(2, result.total);
        LuceneTest.compareHits(result, "A-MU", "A-MQU");
    }
}

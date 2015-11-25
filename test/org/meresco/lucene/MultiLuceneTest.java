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

import static org.junit.Assert.*;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.facet.FacetField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.meresco.lucene.LuceneResponse.Hit;
import org.meresco.lucene.search.TermFrequencySimilarity;

import com.sun.corba.se.spi.orbutil.fsm.Guard.Result;

public class MultiLuceneTest extends SeecrTestCase {

    private Lucene luceneA;
    private Lucene luceneB;
    private Lucene luceneC;
    private MultiLucene multiLucene;

    @SuppressWarnings({ "unchecked", "serial", "rawtypes" })
    @Before
    public void setUp() throws Exception {
        super.setUp();
        LuceneSettings settings = new LuceneSettings();
        LuceneSettings settingsLuceneC = new LuceneSettings();
        settingsLuceneC.similarity = new TermFrequencySimilarity();

        luceneA = new Lucene("coreA", new File(this.tmpDir, "a"), settings);
        luceneB = new Lucene("coreB", new File(this.tmpDir, "b"), settings);
        luceneC = new Lucene("coreC", new File(this.tmpDir, "c"), settingsLuceneC);

        multiLucene = new MultiLucene(new ArrayList<Lucene>() {{ add(luceneA); add(luceneB); add(luceneC);}});

        addDocument(luceneA, "A",      new HashMap() {{put("A", 1);}}, new HashMap() {{put("M", "false"); put("Q", "false"); put("U", "false"); put("S", "1");}});
        addDocument(luceneA, "A-U",    new HashMap() {{put("A", 2 );}}, new HashMap() {{put("M", "false"); put("Q", "false"); put("U", "true" ); put("S", "2");}});
        addDocument(luceneA, "A-Q",    new HashMap() {{put("A", 3 );}}, new HashMap() {{put("M", "false"); put("Q", "true" ); put("U", "false"); put("S", "3");}});
        addDocument(luceneA, "A-QU",   new HashMap() {{put("A", 4 );}}, new HashMap() {{put("M", "false"); put("Q", "true" ); put("U", "true" ); put("S", "4");}});
        addDocument(luceneA, "A-M",    new HashMap() {{put("A", 5 ); put("C", 5);}}, new HashMap() {{put("M", "true" ); put("Q", "false"); put("U", "false"); put("S", "5");}});
        addDocument(luceneA, "A-MU",   new HashMap() {{put("A", 6 ); put("C", 12);}}, new HashMap() {{put("M", "true" ); put("Q", "false"); put("U", "true" ); put("S", "6");}});
        addDocument(luceneA, "A-MQ",   new HashMap() {{put("A", 7 );}}, new HashMap() {{put("M", "true" ); put("Q", "true" ); put("U", "false"); put("S", "7");}});
        addDocument(luceneA, "A-MQU",  new HashMap() {{put("A", 8 );}}, new HashMap() {{put("M", "true" ); put("Q", "true" ); put("U", "true" ); put("S", "8");}});

        addDocument(luceneB, "B-N>A-M",   new HashMap() {{put("B", 5 ); put("D", 5);}}, new HashMap() {{put("N", "true" ); put("O", "true" ); put("P", "false"); put("T", "A"); put("intField", "1");}});
        addDocument(luceneB, "B-N>A-MU",  new HashMap() {{put("B", 6 );}}, new HashMap() {{put("N", "true" ); put("O", "false"); put("P", "false"); put("T", "B"); put("intField", "2");}});
        addDocument(luceneB, "B-N>A-MQ",  new HashMap() {{put("B", 7 );}}, new HashMap() {{put("N", "true" ); put("O", "true" ); put("P", "false"); put("T", "C"); put("intField", "3");}});
        addDocument(luceneB, "B-N>A-MQU", new HashMap() {{put("B", 8 );}}, new HashMap() {{put("N", "true" ); put("O", "false"); put("P", "false"); put("T", "D"); put("intField", "4");}});
        addDocument(luceneB, "B-N",       new HashMap() {{put("B", 9 );}}, new HashMap() {{put("N", "true" ); put("O", "true" ); put("P", "false"); put("T", "E"); put("intField", "5");}});
        addDocument(luceneB, "B",         new HashMap() {{put("B", 10);}}, new HashMap() {{put("N", "false"); put("O", "false"); put("P", "false"); put("T", "F"); put("intField", "6");}});
        addDocument(luceneB, "B-P>A-M",   new HashMap() {{put("B", 5 );}}, new HashMap() {{put("N", "false"); put("O", "true" ); put("P", "true" ); put("T", "G"); put("intField", "7");}});
        addDocument(luceneB, "B-P>A-MU",  new HashMap() {{put("B", 6 );}}, new HashMap() {{put("N", "false"); put("O", "false"); put("P", "true" ); put("T", "H"); put("intField", "8");}});
        addDocument(luceneB, "B-P>A-MQ",  new HashMap() {{put("B", 7 );}}, new HashMap() {{put("N", "false"); put("O", "false" ); put("P", "true" ); put("T", "I"); put("intField", "9");}});
        addDocument(luceneB, "B-P>A-MQU", new HashMap() {{put("B", 8 );}}, new HashMap() {{put("N", "false"); put("O", "false"); put("P", "true" ); put("T", "J"); put("intField", "10");}});
        addDocument(luceneB, "B-P",       new HashMap() {{put("B", 11);}}, new HashMap() {{put("N", "false"); put("O", "true" ); put("P", "true" ); put("T", "K"); put("intField", "11");}});

        addDocument(luceneC, "C-R", new HashMap() {{put("C", 5); put("C2", 12);}}, new HashMap() {{put("R", "true");}});
        addDocument(luceneC, "C-S", new HashMap() {{put("C", 8);}}, new HashMap() {{put("S", "true");}});
        addDocument(luceneC, "C-S2", new HashMap() {{put("C", 7);}}, new HashMap() {{put("S", "false");}});

        luceneA.realCommit();
        luceneB.realCommit();
        luceneC.realCommit();

        settings.commitCount = 1;
        settingsLuceneC.commitCount = 1;
    }

    @After
    public void tearDown() throws Exception {
        luceneA.close();
        luceneB.close();
        luceneC.close();
        super.tearDown();
    }

    @Test
    public void testQueryOneIndexWithComposedQuery() throws Exception {
        ComposedQuery q = new ComposedQuery("coreA", new TermQuery(new Term("Q", "true")));
        LuceneResponse result = multiLucene.executeComposedQuery(q);
        assertEquals(4, result.total);
        compareHits(result, "A-Q", "A-QU", "A-MQ", "A-MQU");
    }

    @Test
    public void testJoinQuery() throws Exception {
        ComposedQuery q = new ComposedQuery("coreA", new MatchAllDocsQuery());
        q.setCoreQuery("coreB", new TermQuery(new Term("N", "true")));
        q.addMatch("coreA", "coreB", "A", "B");
        LuceneResponse result = multiLucene.executeComposedQuery(q);
        assertEquals(4, result.total);
        compareHits(result, "A-M", "A-MU", "A-MQ", "A-MQU");
    }
    
//    testMultipleJoinQueriesKeepsCachesWithinMaxSize
    @Test
    public void testJoinQueryWithFilters() throws Exception {
        ComposedQuery q = new ComposedQuery("coreA");
        q.addFilterQuery("coreB", new TermQuery(new Term("N", "true")));
        q.addMatch("coreA", "coreB", "A", "B");
        
        LuceneResponse result = multiLucene.executeComposedQuery(q);
        assertEquals(4, result.total);
        compareHits(result, "A-M", "A-MU", "A-MQ", "A-MQU");
    }
//    testInfoOnQuery
    
    @Test
    public void testJoinWithFacetInResultCore() throws Exception {
        ComposedQuery q = new ComposedQuery("coreA", new TermQuery(new Term("Q", "true")));
        q.setCoreQuery("coreB", new TermQuery(new Term("O", "true")));
        q.addFacet("coreA", new QueryStringToQuery.FacetRequest("cat_M", 10));
        q.addMatch("coreA", "coreB", "A", "B");
        
        LuceneResponse result = multiLucene.executeComposedQuery(q);
        assertEquals(1, result.total);
        assertEquals(1, result.drilldownData.size());
        assertEquals("true", result.drilldownData.get(0).terms.get(0).label);
        assertEquals(1, result.drilldownData.get(0).terms.get(0).value.intValue());
    }
    
    @Test
    public void testJoinFacet() throws Exception {
        ComposedQuery q = new ComposedQuery("coreA", new TermQuery(new Term("Q", "true")));
        q.addFacet("coreB", new QueryStringToQuery.FacetRequest("cat_N", 10));
        q.addFacet("coreB", new QueryStringToQuery.FacetRequest("cat_O", 10));
        q.addMatch("coreA", "coreB", "A", "B");
        
        LuceneResponse result = multiLucene.executeComposedQuery(q);
        assertEquals(2, result.drilldownData.size());
        assertEquals("cat_N", result.drilldownData.get(0).fieldname);
        assertEquals("true", result.drilldownData.get(0).terms.get(0).label);
        assertEquals("false", result.drilldownData.get(0).terms.get(1).label);
        assertEquals(2, result.drilldownData.get(0).terms.get(0).value.intValue());
        assertEquals(2, result.drilldownData.get(0).terms.get(1).value.intValue());
        assertEquals("cat_O", result.drilldownData.get(1).fieldname);
        assertEquals("false", result.drilldownData.get(1).terms.get(0).label);
        assertEquals("true", result.drilldownData.get(1).terms.get(1).label);
        assertEquals(3, result.drilldownData.get(1).terms.get(0).value.intValue());
        assertEquals(1, result.drilldownData.get(1).terms.get(1).value.intValue());
    }
//    testJoinFacetWithDrilldownQueryFilters
//    testJoinFacetWithJoinDrilldownQueryFilters
//    testJoinDrilldownQueryFilters
//    testJoinFacetWithFilter
//    testJoinFacetFromBPointOfView
//    testJoinFacetWillNotFilter
//    testJoinFacetAndQuery
//    testCoreInfo
    
    @Test
    public void testUniteResultFromTwoIndexes() throws Exception {
        ComposedQuery q = new ComposedQuery("coreA");
        q.setCoreQuery("coreA", new TermQuery(new Term("Q", "true")));
        q.setCoreQuery("coreB", null);
        q.addMatch("coreA", "coreB", "A", "B");
        q.addUnite("coreA", new TermQuery(new Term("U", "true")), "coreB", new TermQuery(new Term("N", "true")));
        LuceneResponse result = multiLucene.executeComposedQuery(q);
        assertEquals(3, result.total);
        compareHits(result, "A-QU", "A-MQ", "A-MQU");
    }
//    testUniteResultFromTwoIndexesCached
//    testUniteResultFromTwoIndexesCachedAfterUpdate
    @Test
    public void testUniteResultFromTwoIndexes_filterQueries() throws Exception {
        ComposedQuery q = new ComposedQuery("coreA");
        q.addFilterQuery("coreA", new TermQuery(new Term("Q", "true")));
        q.addMatch("coreA", "coreB", "A", "B");
        q.addUnite("coreA", new TermQuery(new Term("U", "true")), "coreB", new TermQuery(new Term("N", "true")));
        LuceneResponse result = multiLucene.executeComposedQuery(q);
        assertEquals(3, result.total);
        compareHits(result, "A-QU", "A-MQ", "A-MQU");
    }
//    testUniteAndFacets
//    testUniteAndFacetsWithForeignQuery
//    testUniteAndFacetsWithForeignQueryWithSpecialFacetsQuery
//    testUniteMakesItTwoCoreQuery
    @Test
    public void testStartStopSortKeys() throws Exception {
        ComposedQuery q = new ComposedQuery("coreA");
        q.setCoreQuery("coreA", new TermQuery(new Term("Q", "true")));
        q.setCoreQuery("coreB", null);
        q.addMatch("coreA", "coreB", "A", "B");
        q.addUnite("coreA", new TermQuery(new Term("U", "true")), "coreB", new TermQuery(new Term("N", "true")));
        q.sort = new Sort(new SortField("S", SortField.Type.STRING, false));
        LuceneResponse result = multiLucene.executeComposedQuery(q);
        assertEquals(3, result.total);
        compareHits(result, "A-QU", "A-MQ", "A-MQU");
        
        q.sort = new Sort(new SortField("S", SortField.Type.STRING, true));
        q.stop = 2;
        result = multiLucene.executeComposedQuery(q);
        assertEquals(3, result.total);
        compareHits(result, "A-MQ", "A-MQU");
        
        q.start = 1;
        q.stop = 10;
        result = multiLucene.executeComposedQuery(q);
        assertEquals(3, result.total);
        compareHits(result, "A-MQ", "A-QU");
    }
//    testCachingCollectorsAfterUpdate
//    testCachingCollectorsAfterUpdateInSegmentWithMultipleDocuments
//    testCachingCollectorsAfterDelete
    
    @Test
    public void testJoinQueryOnOptionalKey() throws Exception {
        ComposedQuery q = new ComposedQuery("coreA");
        q.setCoreQuery("coreB", new TermQuery(new Term("N", "true")));
        q.addMatch("coreA", "coreB", "C", "B");
        
        LuceneResponse result = multiLucene.executeComposedQuery(q);
        assertEquals(1, result.total);
        compareHits(result, "A-M");
    }
    
    @Test
    public void testJoinQueryOnOptionalKeyOtherSide() throws Exception {
        ComposedQuery q = new ComposedQuery("coreA");
        q.setCoreQuery("coreB", new TermQuery(new Term("N", "true")));
        q.addMatch("coreA", "coreB", "A", "D");
        
        LuceneResponse result = multiLucene.executeComposedQuery(q);
        assertEquals(1, result.total);
        compareHits(result, "A-M");
    }

    @Test
    public void testJoinQueryThreeCores() throws Exception {
        ComposedQuery q = new ComposedQuery("coreA");
        q.setCoreQuery("coreB", new TermQuery(new Term("N", "true")));
        q.setCoreQuery("coreC", new TermQuery(new Term("R", "true")));
        q.addFacet("coreA", new QueryStringToQuery.FacetRequest("cat_M", 10));
        q.addFacet("coreB", new QueryStringToQuery.FacetRequest("cat_N", 10));
        q.addFacet("coreC", new QueryStringToQuery.FacetRequest("cat_R", 10));
        q.addMatch("coreA", "coreB", "A", "B");
        q.addMatch("coreA", "coreC", "A", "C");
        
        LuceneResponse result = multiLucene.executeComposedQuery(q);
        assertEquals(1, result.total);
        compareHits(result, "A-M");
        assertEquals(3, result.drilldownData.size());
        assertEquals("cat_M", result.drilldownData.get(0).fieldname);
        assertEquals(1, result.drilldownData.get(0).terms.size());
        assertEquals("true", result.drilldownData.get(0).terms.get(0).label);
        assertEquals(1, result.drilldownData.get(0).terms.get(0).value.intValue());
        assertEquals("cat_N", result.drilldownData.get(1).fieldname);
        assertEquals(1, result.drilldownData.get(1).terms.size());
        assertEquals("true", result.drilldownData.get(1).terms.get(0).label);
        assertEquals(1, result.drilldownData.get(1).terms.get(0).value.intValue());
        assertEquals("cat_R", result.drilldownData.get(2).fieldname);
        assertEquals(1, result.drilldownData.get(2).terms.size());
        assertEquals("true", result.drilldownData.get(2).terms.get(0).label);
        assertEquals(1, result.drilldownData.get(2).terms.get(0).value.intValue());
    }
//    testRankQuery
//    testMultipleRankQuery
//    testScoreCollectorCacheInvalidation
//    testNullIteratorOfPForDeltaIsIgnoredInFinalKeySet
//    testKeyFilterIgnoresKeysOutOfBoundsOfKeySet
//    testCollectScoresWithNoResultAndBooleanQueryDoesntFailOnFakeScorerInAggregateScoreCollector
//    testCachingKeyCollectorsIntersectsWithACopyOfTheKeys
//    testTwoCoreQueryWithThirdCoreDrilldownWithOtherCore
//    testFilterQueryInTwoDifferentCores
//    testScoreCollectorOnDifferentKeys
//    testJoinSort
//    testSortWithJoinField
    
    private void compareHits(LuceneResponse response, String... hitIds) {
        Set<String> responseHitIds = new HashSet<String>();
        for (Hit hit : response.hits)
            responseHitIds.add(hit.id);
        Set<String> expectedHitIds = new HashSet<String>();
        for (String hitId : hitIds)
            expectedHitIds.add(hitId);
        assertEquals(expectedHitIds, responseHitIds);
    }

    private void addDocument(Lucene lucene, String identifier, Map<String, Integer> keys, Map<String, String> fields) throws IOException {
        Document doc = new Document();
        for (String keyField : keys.keySet())
            doc.add(new NumericDocValuesField(keyField, keys.get(keyField)));
        for (String fieldname : fields.keySet())
            if (fieldname.equals("intField"))
                doc.add(new IntField(fieldname, Integer.parseInt(fields.get(fieldname)), Store.NO));
            else {
                doc.add(new StringField(fieldname, fields.get(fieldname), Store.NO));
                doc.add(new FacetField("cat_" + fieldname, fields.get(fieldname)));
            }
        lucene.addDocument(identifier, doc);
        lucene.commit();
    }
}

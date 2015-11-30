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
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.facet.FacetField;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryWrapperFilter;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.spell.SuggestWord;
import org.apache.lucene.util.OpenBitSet;
import org.junit.Before;
import org.junit.Test;
import org.junit.internal.runners.statements.Fail;
import org.meresco.lucene.Lucene.TermCount;
import org.meresco.lucene.LuceneResponse.Hit;
import org.meresco.lucene.QueryConverter.FacetRequest;
import org.meresco.lucene.search.join.AggregateScoreSuperCollector;
import org.meresco.lucene.search.join.KeySuperCollector;
import org.meresco.lucene.search.join.ScoreSuperCollector;

import com.sun.corba.se.spi.orbutil.fsm.Guard.Result;

public class LuceneTest extends SeecrTestCase {

    private Lucene lucene;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        LuceneSettings settings = new LuceneSettings();
        settings.commitCount = 1;
        lucene = new Lucene(this.tmpDir, settings);
    }

    @Test
    public void testAddDocument() throws Exception {
        Document doc = new Document();
        doc.add(new StringField("naam", "waarde", Store.NO));
        lucene.addDocument("id1", doc);
        LuceneResponse response = lucene.executeQuery(new MatchAllDocsQuery());
        assertEquals(1, response.total);
        assertEquals(1, response.hits.size());
        assertEquals("id1", response.hits.get(0).id);
    }

    @Test
    public void testAddDeleteDocument() throws Exception {
        lucene.addDocument("id1", new Document());
        lucene.addDocument("id2", new Document());
        lucene.addDocument("id3", new Document());
        lucene.deleteDocument("id1");
        LuceneResponse response = lucene.executeQuery(new MatchAllDocsQuery());
        assertEquals(2, response.total);
        assertEquals(2, response.hits.size());
    }

    @Test
    public void testAddTwiceUpdatesDocument() throws Exception {
        Document doc1 = new Document();
        doc1.add(new StringField("field0", "value0", Store.NO));
        doc1.add(new StringField("field1", "value1", Store.NO));
        Document doc2 = new Document();
        doc2.add(new StringField("field0", "value0", Store.NO));
        lucene.addDocument("id:0", doc1);
        lucene.addDocument("id:0", doc2);
        LuceneResponse result = lucene.executeQuery(new TermQuery(new Term("field0", "value0")));
        assertEquals(1, result.total);
        result = lucene.executeQuery(new TermQuery(new Term("field1", "value1")));
        assertEquals(0, result.total);
    }

    @Test
    public void testFacets() throws Exception {
        Document doc1 = new Document();
        doc1.add(new StringField("field1", "id0", Store.NO));
        doc1.add(new FacetField("facet-field2", "first item0"));
        doc1.add(new FacetField("facet-field3", "second item"));

        Document doc2 = new Document();
        doc2.add(new StringField("field1", "id1", Store.NO));
        doc2.add(new FacetField("facet-field2", "first item1"));
        doc2.add(new FacetField("facet-field3", "other value"));

        Document doc3 = new Document();
        doc3.add(new StringField("field1", "id2", Store.NO));
        doc3.add(new FacetField("facet-field2", "first item2"));
        doc3.add(new FacetField("facet-field3", "second item"));

        lucene.addDocument("id0", doc1);
        lucene.addDocument("id1", doc2);
        lucene.addDocument("id2", doc3);

        LuceneResponse result = lucene.executeQuery(new MatchAllDocsQuery());
        assertEquals(3, result.total);
        assertEquals(0, result.drilldownData.size());

        ArrayList<FacetRequest> facets = new ArrayList<FacetRequest>();
        facets.add(new FacetRequest("facet-field2", 10));
        result = lucene.executeQuery(new MatchAllDocsQuery(), facets);
        assertEquals(3, result.total);
        assertEquals(1, result.drilldownData.size());
        assertEquals("facet-field2", result.drilldownData.get(0).fieldname);
        assertEquals(3, result.drilldownData.get(0).terms.size());
        assertEquals("first item0", result.drilldownData.get(0).terms.get(0).label);
        assertEquals("first item1", result.drilldownData.get(0).terms.get(1).label);
        assertEquals("first item2", result.drilldownData.get(0).terms.get(2).label);
        assertEquals(1, result.drilldownData.get(0).terms.get(0).count);
        assertEquals(1, result.drilldownData.get(0).terms.get(1).count);
        assertEquals(1, result.drilldownData.get(0).terms.get(2).count);

        facets = new ArrayList<FacetRequest>();
        facets.add(new FacetRequest("facet-field3", 10));
        result = lucene.executeQuery(new MatchAllDocsQuery(), facets);
        assertEquals(3, result.total);
        assertEquals(1, result.drilldownData.size());
        assertEquals("facet-field3", result.drilldownData.get(0).fieldname);
        assertEquals(2, result.drilldownData.get(0).terms.size());
        assertEquals("second item", result.drilldownData.get(0).terms.get(0).label);
        assertEquals("other value", result.drilldownData.get(0).terms.get(1).label);
        assertEquals(2, result.drilldownData.get(0).terms.get(0).count);
        assertEquals(1, result.drilldownData.get(0).terms.get(1).count);

        facets.get(0).maxTerms = 0;
        result = lucene.executeQuery(new MatchAllDocsQuery(), facets);
        assertEquals(3, result.total);
        assertEquals(1, result.drilldownData.size());
        assertEquals(2, result.drilldownData.get(0).terms.size());
        
        assertEquals(result.drilldownData, lucene.facets(facets, null, null, null));
    }
    
    @Test
    public void testHierarchicalFacets() throws Exception {
        Document doc1 = new Document();
        doc1.add(new StringField("field1", "id0", Store.NO));
        doc1.add(new FacetField("facet-field", "first", "second"));
        doc1.add(new FacetField("facet-field", "subfirst", "subsecond"));
        this.lucene.facetsConfig.setHierarchical("facet-field", true);
        this.lucene.facetsConfig.setMultiValued("facet-field", true);
        this.lucene.addDocument("id1", doc1);
        
        ArrayList<FacetRequest> facets = new ArrayList<FacetRequest>();
        FacetRequest facet = new FacetRequest("facet-field", 10);
        facet.path = new String[] {"first"};
        facets.add(facet);
        
        LuceneResponse result = lucene.executeQuery(new MatchAllDocsQuery(), facets);
        assertEquals(1, result.total);
        assertEquals(1, result.drilldownData.size());
        assertEquals("facet-field", result.drilldownData.get(0).fieldname);
        assertEquals(1, result.drilldownData.get(0).terms.size());
        assertEquals("second", result.drilldownData.get(0).terms.get(0).label);
    }

    @Test
    public void testFacetsInDifferentIndexFieldName() throws Exception {
        FacetsConfig facetsConfig = lucene.getSettings().facetsConfig;
        facetsConfig.setIndexFieldName("field0", "$facets_1");
        facetsConfig.setIndexFieldName("field2", "$facets_1");

        Document doc1 = new Document();
        doc1.add(new FacetField("field0", "value0"));
        doc1.add(new FacetField("field1", "value1"));
        doc1.add(new FacetField("field2", "value2"));
        lucene.addDocument("id0", doc1);

        ArrayList<FacetRequest> facets = new ArrayList<FacetRequest>();
        facets.add(new FacetRequest("field0", 10));
        facets.add(new FacetRequest("field1", 10));
        facets.add(new FacetRequest("field2", 10));
        LuceneResponse result = lucene.executeQuery(new MatchAllDocsQuery(), facets);
        assertEquals(3, result.drilldownData.size());
        assertEquals("field0", result.drilldownData.get(0).fieldname);
        assertEquals(1, result.drilldownData.get(0).terms.size());
        assertEquals("field1", result.drilldownData.get(1).fieldname);
        assertEquals(1, result.drilldownData.get(1).terms.size());
        assertEquals("field2", result.drilldownData.get(2).fieldname);
        assertEquals(1, result.drilldownData.get(2).terms.size());
    }

    @SuppressWarnings("serial")
    @Test
    public void testFacetIndexFieldNames() throws Exception {
        FacetsConfig facetsConfig = lucene.getSettings().facetsConfig;
        facetsConfig.setIndexFieldName("field0", "$facets_1");
        facetsConfig.setIndexFieldName("field2", "$facets_1");

        ArrayList<FacetRequest> facets = new ArrayList<FacetRequest>();
        facets.add(new FacetRequest("field0", 10));
        facets.add(new FacetRequest("field1", 10));
        facets.add(new FacetRequest("field2", 10));

        assertArrayEquals(new String[] {"$facets", "$facets_1"}, lucene.getIndexFieldNames(facets));
        assertArrayEquals(new String[] {"$facets_1"}, lucene.getIndexFieldNames(new ArrayList<FacetRequest>() {{ add(new FacetRequest("field0", 10)); }}));
        assertArrayEquals(new String[] {"$facets"}, lucene.getIndexFieldNames(new ArrayList<FacetRequest>() {{ add(new FacetRequest("field1", 10)); }}));
        assertArrayEquals(new String[] {"$facets_1"}, lucene.getIndexFieldNames(new ArrayList<FacetRequest>() {{ add(new FacetRequest("field0", 10)); add(new FacetRequest("field2", 10)); }}));
    }

    @Test
    public void testSorting() throws Exception {
        Document doc1 = new Document();
        doc1.add(new StringField("field1", "AA", Store.NO));
        lucene.addDocument("id1", doc1);

        Document doc2 = new Document();
        doc2.add(new StringField("field1", "BB", Store.NO));
        lucene.addDocument("id2", doc2);

        Document doc3 = new Document();
        doc3.add(new StringField("field1", "CC", Store.NO));
        lucene.addDocument("id3", doc3);

        Sort sort = new Sort();
        sort.setSort(new SortField("field1", SortField.Type.STRING, false));
        QueryData q = new QueryData();
        q.sort = sort;
        LuceneResponse result = lucene.executeQuery(q, null, null, null, null, null);
        assertEquals(3, result.total);
        assertEquals("id1", result.hits.get(0).id);
        assertEquals("id2", result.hits.get(1).id);
        assertEquals("id3", result.hits.get(2).id);

        sort = new Sort();
        sort.setSort(new SortField("field1", SortField.Type.STRING, true));
        q.sort = sort;
        result = lucene.executeQuery(q, null, null, null, null, null);
        assertEquals(3, result.total);
        assertEquals("id3", result.hits.get(0).id);
        assertEquals("id2", result.hits.get(1).id);
        assertEquals("id1", result.hits.get(2).id);
    }

    @Test
    public void testCommitTimer() throws Exception {
        lucene.close();
        LuceneSettings settings = new LuceneSettings();
        settings.commitTimeout = 1;
        lucene = new Lucene(this.tmpDir, settings);
        lucene.addDocument("id1", new Document());
        Thread.sleep(500);
        assertEquals(0, lucene.executeQuery(new MatchAllDocsQuery()).total);
        Thread.sleep(550);
        assertEquals(1, lucene.executeQuery(new MatchAllDocsQuery()).total);
    }

    @Test
    public void testCommitTimerAndCount() throws Exception {
        lucene.close();
        LuceneSettings settings = new LuceneSettings();
        settings.commitTimeout = 1;
        settings.commitCount = 2;
        lucene = new Lucene(this.tmpDir, settings);
        lucene.addDocument("id1", new Document());
        lucene.addDocument("id2", new Document());
        assertEquals(2, lucene.executeQuery(new MatchAllDocsQuery()).total);
        lucene.addDocument("id3", new Document());
        Thread.sleep(1050);
        assertEquals(3, lucene.executeQuery(new MatchAllDocsQuery()).total);
    }

    @Test
    public void testStartStop() throws Exception {
        Document doc1 = new Document();
        doc1.add(new StringField("field1", "AA", Store.NO));
        lucene.addDocument("id1", doc1);

        Document doc2 = new Document();
        doc2.add(new StringField("field1", "BB", Store.NO));
        lucene.addDocument("id2", doc2);

        Document doc3 = new Document();
        doc3.add(new StringField("field1", "CC", Store.NO));
        lucene.addDocument("id3", doc3);

        LuceneResponse result = lucene.executeQuery(new MatchAllDocsQuery());
        assertEquals(3, result.total);
        assertEquals(3, result.hits.size());
        result = lucene.executeQuery(new MatchAllDocsQuery(), 1, 10);
        assertEquals(3, result.total);
        assertEquals(2, result.hits.size());
        result = lucene.executeQuery(new MatchAllDocsQuery(), 0, 2);
        assertEquals(3, result.total);
        assertEquals(2, result.hits.size());
        result = lucene.executeQuery(new MatchAllDocsQuery(), 2, 2);
        assertEquals(3, result.total);
        assertEquals(0, result.hits.size());
        result = lucene.executeQuery(new MatchAllDocsQuery(), 1, 2);
        assertEquals(3, result.total);
        assertEquals(1, result.hits.size());
        result = lucene.executeQuery(new MatchAllDocsQuery(), 0, 0);
        assertEquals(3, result.total);
        assertEquals(0, result.hits.size());
    }

    @SuppressWarnings("serial")
    @Test
    public void testQueryWithFilter() throws Exception {
        Document doc1 = new Document();
        doc1.add(new StringField("field1", "value0", Store.NO));
        lucene.addDocument("id1", doc1);

        Document doc2 = new Document();
        doc2.add(new StringField("field1", "value1", Store.NO));
        lucene.addDocument("id2", doc2);

        assertEquals(2, lucene.executeQuery(new MatchAllDocsQuery(), 0, 0).total);
        final Filter f = new QueryWrapperFilter(new TermQuery(new Term("field1", "value1")));
        assertEquals(1, lucene.executeQuery(new QueryData(), null, null, new ArrayList<Filter>() {{ add(f); }}, null, null).total);
    }
    
    @SuppressWarnings("serial")
    @Test
    public void testQueryWithFilterQuery() throws Exception {
        Document doc1 = new Document();
        doc1.add(new StringField("field1", "value0", Store.NO));
        lucene.addDocument("id1", doc1);

        Document doc2 = new Document();
        doc2.add(new StringField("field1", "value1", Store.NO));
        lucene.addDocument("id2", doc2);

        assertEquals(2, lucene.executeQuery(new MatchAllDocsQuery(), 0, 0).total);
        final Query f = new TermQuery(new Term("field1", "value1"));
        assertEquals(1, lucene.executeQuery(new QueryData(), new ArrayList<Query>() {{ add(f); }}, null, null, null, null).total);
    }

    @SuppressWarnings("serial")
    @Test
    public void testQueryWithKeyCollectors() throws Exception {
        Document doc1 = new Document();
        doc1.add(new StringField("field0", "value", Store.NO));
        doc1.add(new NumericDocValuesField("field1", 1));
        lucene.addDocument("id1", doc1);

        Document doc2 = new Document();
        doc2.add(new NumericDocValuesField("field1", 2));
        lucene.addDocument("id2", doc2);

        final KeySuperCollector k = new KeySuperCollector("field1");
        assertEquals(2, lucene.executeQuery(new QueryData(), null, null, null, null, new ArrayList<KeySuperCollector>() {{ add(k); }}).total);

        OpenBitSet collectedKeys = k.getCollectedKeys();
        assertEquals(false, collectedKeys.get(0));
        assertEquals(true, collectedKeys.get(1));
        assertEquals(true, collectedKeys.get(2));
        assertEquals(false, collectedKeys.get(3));
        assertEquals(collectedKeys, lucene.collectKeys(null, "field1", new MatchAllDocsQuery()));

        final KeySuperCollector k1 = new KeySuperCollector("field1");
        TermQuery field0Query = new TermQuery(new Term("field0", "value"));
        QueryData q = new QueryData();
        q.query = field0Query;
        assertEquals(1, lucene.executeQuery(q, null, null, null, null, new ArrayList<KeySuperCollector>() {{ add(k1); }}).total);

        OpenBitSet keysWithFilter = k1.getCollectedKeys();
        assertEquals(false, keysWithFilter.get(0));
        assertEquals(true, keysWithFilter.get(1));
        assertEquals(false, keysWithFilter.get(2));
        assertEquals(keysWithFilter, lucene.collectKeys(field0Query, "field1", new MatchAllDocsQuery()));

    }

    @SuppressWarnings("serial")
    @Test
    public void testQueryWithScoreCollectors() throws Exception {
        Document doc1 = new Document();
        doc1.add(new StringField("field0", "value", Store.NO));
        doc1.add(new NumericDocValuesField("field1", 1));
        lucene.addDocument("id1", doc1);

        Document doc2 = new Document();
        doc2.add(new NumericDocValuesField("field1", 2));
        lucene.addDocument("id2", doc2);

        final ScoreSuperCollector scoreCollector = this.lucene.scoreCollector("field1", new MatchAllDocsQuery());
        assertEquals(1.0, scoreCollector.score(1), 0);
        assertEquals(1.0, scoreCollector.score(2), 0);
        
        final AggregateScoreSuperCollector aggregator = new AggregateScoreSuperCollector("field1", new ArrayList<ScoreSuperCollector>() {{add(scoreCollector);}});
        ArrayList<AggregateScoreSuperCollector> aggregators = new ArrayList<AggregateScoreSuperCollector>() {{add(aggregator);}};
        LuceneResponse result = this.lucene.executeQuery(new QueryData(), null, null, null, aggregators, null);
        assertEquals(2, result.total);
        assertEquals(2, result.hits.get(0).score, 0);
        assertEquals(2, result.hits.get(1).score, 0);
    }
    
    @Test
    public void testPrefixSearch() throws Exception {
        Document doc1 = new Document();
        doc1.add(new StringField("field1", "value0", Store.NO));
        lucene.addDocument("id1", doc1);

        Document doc2 = new Document();
        doc2.add(new StringField("field1", "value1", Store.NO));
        lucene.addDocument("id2", doc2);

        Document doc3 = new Document();
        doc3.add(new StringField("field1", "value2", Store.NO));
        lucene.addDocument("id3", doc3);

        List<TermCount> terms = lucene.termsForField("field1", "val", 10);
        assertEquals(3, terms.size());
        assertEquals("value0", terms.get(0).term);
        assertEquals(1, terms.get(0).count);
        assertEquals("value1", terms.get(1).term);
        assertEquals(1, terms.get(0).count);
        assertEquals("value2", terms.get(2).term);
        assertEquals(1, terms.get(0).count);
    }

    @Test
    public void testNumDocs() throws Exception {
        assertEquals(0, lucene.maxDoc());
        assertEquals(0, lucene.numDocs());
        Document doc1 = new Document();
        doc1.add(new StringField("field1", "value0", Store.NO));
        lucene.addDocument("id1", doc1);
        assertEquals(1, lucene.maxDoc());
        assertEquals(1, lucene.numDocs());
    }

    @SuppressWarnings("serial")
    @Test
    public void testFieldname() throws IOException {
        Document doc1 = new Document();
        doc1.add(new StringField("field1", "value0", Store.NO));
        lucene.addDocument("id1", doc1);

        List<String> fieldnames = lucene.fieldnames();
        assertEquals(2, fieldnames.size());
        assertEquals(new ArrayList<String>() {{ add("__id__"); add("field1"); }}, fieldnames);
    }

    @SuppressWarnings("serial")
    @Test
    public void testDrilldownFieldname() throws IOException {
        Document doc1 = new Document();
        doc1.add(new FacetField("cat", "cat 1"));
        lucene.addDocument("id1", doc1);

        Document doc2 = new Document();
        doc2.add(new FacetField("cat", "cat 2"));
        lucene.addDocument("id2", doc2);

        List<String> fieldnames = lucene.drilldownFieldnames(50, null);
        assertEquals(1, fieldnames.size());
        assertEquals(new ArrayList<String>() {{ add("cat"); }}, fieldnames);

        fieldnames = lucene.drilldownFieldnames(50, "cat");
        assertEquals(2, fieldnames.size());
        assertEquals(new ArrayList<String>() {{ add("cat 2"); add("cat 1");}}, fieldnames);
    }
    
    @SuppressWarnings("serial")
    @Test
    public void testSuggestions() throws Exception {
        addDocument(lucene, "id:0", null, new HashMap() {{put("field1", "value0"); put("field2", "value2" ); put("field5", "value2" );}});
        
        assertEquals("value2", lucene.suggest("value0", 2, "field5")[0].string);
        assertEquals("value2", lucene.suggest("valeu", 2, "field5")[0].string);
        
        QueryConverter.SuggestionRequest sr = new QueryConverter.SuggestionRequest("field5", 2);
        sr.add("value0");
        sr.add("valeu");
        QueryData q = new QueryData();
        q.suggestionRequest = sr;
        LuceneResponse response = lucene.executeQuery(q, null, null, null, null, null);
        compareHits(response, "id:0");
        HashMap<String, SuggestWord[]> suggestions = new HashMap<>();
        assertEquals("value2", response.suggestions.get("value0")[0].string);
        assertEquals("value2", response.suggestions.get("valeu")[0].string);
    }
    
    @SuppressWarnings({ "serial", "unchecked", "rawtypes" })
    @Test
    public void testDedupFilterCollectorSortedByField() throws Exception {
        addDocument(lucene, "urn:1", new HashMap() {{put("__key__", 42); put("__key__.date", 2012);}}, new HashMap() {{put("field0", "v0"); put("A", "cat-A");}});
        addDocument(lucene, "urn:2", new HashMap() {{put("__key__", 42); put("__key__.date", 2013);}}, new HashMap() {{put("field0", "v1"); put("A", "cat-A");}}); //first hit of 3 duplicates
        addDocument(lucene, "urn:3", new HashMap() {{put("__key__", 42);}}, new HashMap() {{put("field0", "v2"); put("A", "cat-A");}});
        addDocument(lucene, "urn:4", null, new HashMap() {{put("field0", "v3"); put("A", "cat-A");}});
        QueryData q = new QueryData();
        q.query = new MatchAllDocsQuery();
        q.facets = new ArrayList<FacetRequest>() {{add(new FacetRequest("cat_A", 10));}};
        q.dedupField = "__key__";
        q.dedupSortField = "__key__.date";
        LuceneResponse result = lucene.executeQuery(q, null, null, null, null, null);
        // expected two hits: "urn:2" (3x) and "urn:4" in no particular order
        assertEquals(2, result.total);
        assertEquals(4, result.totalWithDuplicates);
        compareHits(result, "urn:2", "urn:4");
        assertEquals("__key__", result.hits.get(0).duplicateField);
        assertEquals(3, result.hits.get(0).duplicateCount);
        assertEquals(1, result.hits.get(1).duplicateCount);
        assertEquals("cat-A", result.drilldownData.get(0).terms.get(0).label);
        assertEquals(4, result.drilldownData.get(0).terms.get(0).count);
    }
    
    @SuppressWarnings({ "serial", "unchecked", "rawtypes" })
    @Test
    public void testGroupingCollector() throws Exception {
        addDocument(lucene, "urn:1", new HashMap() {{put("__key__", 42);}}, new HashMap() {{put("field0", "v0");}});
        addDocument(lucene, "urn:2", new HashMap() {{put("__key__", 42);}}, new HashMap() {{put("field0", "v1");}});
        addDocument(lucene, "urn:3", new HashMap() {{put("__key__", 43);}}, new HashMap() {{put("field0", "v2");}});
        addDocument(lucene, "urn:4", null, new HashMap() {{put("field0", "v3");}});
        
        QueryData q = new QueryData();
        q.stop = 3;
        q.groupingField = "__key__";
        q.sort = new Sort(new SortField("field0", SortField.Type.STRING));
        LuceneResponse result = lucene.executeQuery(q, null, null, null, null, null);
        // expected two hits: "urn:2" (3x) and "urn:4" in no particular order
        assertEquals(4, result.total);
        compareHits(result, "urn:1", "urn:3", "urn:4");
        assertEquals("__key__", result.hits.get(0).groupingField);
        assertEquals(new ArrayList<String>() {{ add("urn:1"); add("urn:2"); }}, result.hits.get(0).duplicates);
        assertEquals(new ArrayList<String>() {{ add("urn:3"); }}, result.hits.get(1).duplicates);
        assertEquals(new ArrayList<String>() {{ add("urn:4"); }}, result.hits.get(2).duplicates);
    }
    
    @SuppressWarnings({ "serial", "unchecked", "rawtypes" })
    @Test
    public void testGroupingOnNonExistingField() throws Exception {
        addDocument(lucene, "urn:1", new HashMap() {{put("__key__", 42);}}, new HashMap() {{put("field0", "v0");}});
        addDocument(lucene, "urn:2", new HashMap() {{put("__key__", 42);}}, new HashMap() {{put("field0", "v1");}});
        addDocument(lucene, "urn:3", new HashMap() {{put("__key__", 43);}}, new HashMap() {{put("field0", "v2");}});
        addDocument(lucene, "urn:4", null, new HashMap() {{put("field0", "v3");}});
     
        QueryData q = new QueryData();
        q.groupingField = "__other_key__";
        q.stop = 3;
        LuceneResponse result = lucene.executeQuery(q, null, null, null, null, null);
        assertEquals(4, result.total);
        assertEquals(3, result.hits.size());
    }
    
    @SuppressWarnings({ "serial", "rawtypes", "unchecked" })
    @Test
    public void testDontGroupIfMaxResultsAreLessThanTotalRecords() throws Exception {
        addDocument(lucene, "urn:1", new HashMap() {{put("__key__", 42);}}, new HashMap() {{put("field0", "v0");}});
        addDocument(lucene, "urn:2", new HashMap() {{put("__key__", 42);}}, new HashMap() {{put("field0", "v1");}});

        QueryData q = new QueryData();
        q.groupingField = "__key__";
        LuceneResponse result = lucene.executeQuery(q, null, null, null, null, null);
        
        assertEquals(2, result.total);
        compareHits(result, "urn:1", "urn:2");
        assertEquals(new ArrayList<String>() {{ add("urn:1"); }}, result.hits.get(0).duplicates);
        assertEquals(new ArrayList<String>() {{ add("urn:2"); }}, result.hits.get(1).duplicates);
    }
    
    @SuppressWarnings({ "serial", "rawtypes", "unchecked" })
    @Test
    public void testGroupingCollectorReturnsMaxHitAfterGrouping() throws Exception {
        addDocument(lucene, "urn:1", new HashMap() {{put("__key__", 42);}}, new HashMap() {{put("field0", "v0");}});
        addDocument(lucene, "urn:2", new HashMap() {{put("__key__", 42);}}, new HashMap() {{put("field0", "v1");}});
        for (int i=3; i<11; i++)
            addDocument(lucene, "urn:" + i, null, new HashMap() {{put("field0", "v0");}});
        
        QueryData q = new QueryData();
        q.groupingField = "__key__";
        q.stop = 5;
        LuceneResponse result = lucene.executeQuery(q, null, null, null, null, null);
        assertEquals(10, result.total);
        assertEquals(5, result.hits.size());
    }
    
    public static void compareHits(LuceneResponse response, String... hitIds) {
        Set<String> responseHitIds = new HashSet<String>();
        for (Hit hit : response.hits)
            responseHitIds.add(hit.id);
        Set<String> expectedHitIds = new HashSet<String>();
        for (String hitId : hitIds)
            expectedHitIds.add(hitId);
        assertEquals(expectedHitIds, responseHitIds);
    }

    public static void compareHitsOrdered(LuceneResponse response, String... hitIds) {
        List<String> responseHitIds = new ArrayList<String>();
        for (Hit hit : response.hits)
            responseHitIds.add(hit.id);
        List<String> expectedHitIds = new ArrayList<String>();
        for (String hitId : hitIds)
            expectedHitIds.add(hitId);
        assertEquals(expectedHitIds, responseHitIds);
    }

    public static void addDocument(Lucene lucene, String identifier, Map<String, Integer> keys, Map<String, String> fields) throws IOException {
        Document doc = new Document();
        if (keys != null)
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

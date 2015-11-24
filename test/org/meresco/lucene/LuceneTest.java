package org.meresco.lucene;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.facet.FacetField;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.QueryWrapperFilter;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.OpenBitSet;
import org.junit.Before;
import org.junit.Test;
import org.meresco.lucene.Lucene.TermCount;
import org.meresco.lucene.QueryStringToQuery.FacetRequest;
import org.meresco.lucene.search.join.KeySuperCollector;

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
        String[] ddTerms = result.drilldownData.get(0).terms.keySet().toArray(new String[0]);
        Arrays.sort(ddTerms);
        assertArrayEquals(new String[] { "first item0", "first item1", "first item2" }, ddTerms);
        assertEquals(1, result.drilldownData.get(0).terms.get("first item0").intValue());
        assertEquals(1, result.drilldownData.get(0).terms.get("first item1").intValue());
        assertEquals(1, result.drilldownData.get(0).terms.get("first item2").intValue());
        
        facets = new ArrayList<FacetRequest>();
        facets.add(new FacetRequest("facet-field3", 10));
        result = lucene.executeQuery(new MatchAllDocsQuery(), facets);
        assertEquals(3, result.total);
        assertEquals(1, result.drilldownData.size());
        assertEquals("facet-field3", result.drilldownData.get(0).fieldname);
        assertEquals(2, result.drilldownData.get(0).terms.size());
        ddTerms = result.drilldownData.get(0).terms.keySet().toArray(new String[0]);
        Arrays.sort(ddTerms);
        assertArrayEquals(new String[] { "other value", "second item" }, ddTerms);
        assertEquals(2, result.drilldownData.get(0).terms.get("second item").intValue());
        assertEquals(1, result.drilldownData.get(0).terms.get("other value").intValue());
        
        assertEquals(result.drilldownData, lucene.facets(facets, null, null));
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
        LuceneResponse result = lucene.executeQuery(new MatchAllDocsQuery(), 0, 10, sort, null, null, null);
        assertEquals(3, result.total);
        assertEquals("id1", result.hits.get(0).id);
        assertEquals("id2", result.hits.get(1).id);
        assertEquals("id3", result.hits.get(2).id);
        
        sort = new Sort();
        sort.setSort(new SortField("field1", SortField.Type.STRING, true));
        result = lucene.executeQuery(new MatchAllDocsQuery(), 0, 10, sort, null, null, null);
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
        result = lucene.executeQuery(new MatchAllDocsQuery(), 1, 10, null, null, null, null);
        assertEquals(3, result.total);
        assertEquals(2, result.hits.size());
        result = lucene.executeQuery(new MatchAllDocsQuery(), 0, 2, null, null, null, null);
        assertEquals(3, result.total);
        assertEquals(2, result.hits.size());
        result = lucene.executeQuery(new MatchAllDocsQuery(), 2, 2, null, null, null, null);
        assertEquals(3, result.total);
        assertEquals(0, result.hits.size());
        result = lucene.executeQuery(new MatchAllDocsQuery(), 1, 2, null, null, null, null);
        assertEquals(3, result.total);
        assertEquals(1, result.hits.size());
        result = lucene.executeQuery(new MatchAllDocsQuery(), 0, 0, null, null, null, null);
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
        
        assertEquals(2, lucene.executeQuery(new MatchAllDocsQuery(), 0, 0, null, null, null, null).total);
        final Filter f = new QueryWrapperFilter(new TermQuery(new Term("field1", "value1")));
        assertEquals(1, lucene.executeQuery(new MatchAllDocsQuery(), 0, 0, null, null, new ArrayList<Filter>() {{ add(f); }}, null).total);
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
        assertEquals(2, lucene.executeQuery(new MatchAllDocsQuery(), 0, 0, null, null, null, new ArrayList<KeySuperCollector>() {{ add(k); }}).total);
        
        OpenBitSet collectedKeys = k.getCollectedKeys();
        assertEquals(false, collectedKeys.get(0));
        assertEquals(true, collectedKeys.get(1));
        assertEquals(true, collectedKeys.get(2));
        assertEquals(false, collectedKeys.get(3));
        assertEquals(collectedKeys, lucene.collectKeys(null, "field1", new MatchAllDocsQuery()));
        
        final KeySuperCollector k1 = new KeySuperCollector("field1");
        TermQuery field0Query = new TermQuery(new Term("field0", "value"));
        assertEquals(1, lucene.executeQuery(field0Query, 0, 0, null, null, null, new ArrayList<KeySuperCollector>() {{ add(k1); }}).total);
        
        OpenBitSet keysWithFilter = k1.getCollectedKeys();
        assertEquals(false, keysWithFilter.get(0));
        assertEquals(true, keysWithFilter.get(1));
        assertEquals(false, keysWithFilter.get(2));
        assertEquals(keysWithFilter, lucene.collectKeys(field0Query, "field1", new MatchAllDocsQuery()));
        
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
}

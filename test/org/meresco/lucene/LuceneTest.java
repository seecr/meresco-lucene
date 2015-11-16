package org.meresco.lucene;

import static org.junit.Assert.*;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.facet.FacetField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.junit.Before;
import org.junit.Test;
import org.meresco.lucene.QueryStringToQuery.FacetRequest;

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
        assertNull(result.drilldownData);
        
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
        LuceneResponse result = lucene.executeQuery(new MatchAllDocsQuery(), 0, 10, sort, null);
        assertEquals(3, result.total);
        assertEquals("id1", result.hits.get(0).id);
        assertEquals("id2", result.hits.get(1).id);
        assertEquals("id3", result.hits.get(2).id);
        
        sort = new Sort();
        sort.setSort(new SortField("field1", SortField.Type.STRING, true));
        result = lucene.executeQuery(new MatchAllDocsQuery(), 0, 10, sort, null);
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
        result = lucene.executeQuery(new MatchAllDocsQuery(), 1, 10, null, null);
        assertEquals(3, result.total);
        assertEquals(2, result.hits.size());
        result = lucene.executeQuery(new MatchAllDocsQuery(), 0, 2, null, null);
        assertEquals(3, result.total);
        assertEquals(2, result.hits.size());
        result = lucene.executeQuery(new MatchAllDocsQuery(), 2, 2, null, null);
        assertEquals(3, result.total);
        assertEquals(0, result.hits.size());
        result = lucene.executeQuery(new MatchAllDocsQuery(), 1, 2, null, null);
        assertEquals(3, result.total);
        assertEquals(1, result.hits.size());
    }
}

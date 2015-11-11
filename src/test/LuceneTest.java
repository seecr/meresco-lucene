package test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.File;
import java.util.ArrayList;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.facet.FacetField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.TermQuery;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.meresco.lucene.Lucene;
import org.meresco.lucene.Lucene.FacetRequest;
import org.meresco.lucene.LuceneResponse;

public class LuceneTest {

    private File tmpDir;

    @Before
    public void setUp() throws Exception {
        this.tmpDir = TestUtils.createTempDirectory();
    }

    @After
    public void tearDown() throws Exception {
        TestUtils.deleteDirectory(this.tmpDir);
    }
    
    @Test
    public void testAddDocument() throws Exception {
        Lucene lucene = new Lucene(this.tmpDir);
        Document doc = new Document();
        doc.add(new StringField("naam", "waarde", Store.NO));
        lucene.addDocument("id1", doc);
        LuceneResponse response = lucene.executeQuery(new MatchAllDocsQuery());
        assertEquals(1, response.total);
        assertEquals(1, response.hits.size());
        assertEquals("id1", response.hits.get(0).id);
    }
    
    @Test
    public void testAddTwiceUpdatesDocument() throws Exception {
        Lucene lucene = new Lucene(this.tmpDir);
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
        Lucene lucene = new Lucene(this.tmpDir);
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
        facets.add(lucene.new FacetRequest("facet-field2", 10));
        result = lucene.executeQuery(new MatchAllDocsQuery(), facets);
        assertEquals(3, result.total);
        assertEquals(1, result.drilldownData.size());
    }
}

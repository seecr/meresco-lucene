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
        Document doc1 = new Document();
        doc1.add(new SortedDocValuesField("sortfieldB", new BytesRef("a")));
        doc1.add(new NumericDocValuesField("intSortfieldB", 1));
        doc1.add(new NumericDocValuesField("doubleSortfieldA", NumericUtils.doubleToSortableLong(1.0)));
        doc1.add(new NumericDocValuesField("keyB", 1));
        luceneB.addDocument(doc1);
        
        Document doc2 = new Document();
        doc2.add(new SortedDocValuesField("sortfieldB", new BytesRef("b")));
        doc2.add(new NumericDocValuesField("intSortfieldB", 2));
        doc2.add(new NumericDocValuesField("doubleSortfieldB", NumericUtils.doubleToSortableLong(2.0)));
        doc2.add(new NumericDocValuesField("keyB", 2));
        luceneB.addDocument(doc2);
        
        Document doc3 = new Document();
        doc3.add(new NumericDocValuesField("keyA", 1));
        doc3.add(new SortedDocValuesField("sortfieldA", new BytesRef("a")));
        doc3.add(new NumericDocValuesField("intSortfieldA", 1));
        doc3.add(new NumericDocValuesField("doubleSortfieldA", NumericUtils.doubleToSortableLong(1.0)));
        luceneA.addDocument("id3", doc3);
        Document doc4 = new Document();
        doc4.add(new NumericDocValuesField("keyA", 2));
        doc4.add(new SortedDocValuesField("sortfieldA", new BytesRef("b")));
        doc4.add(new NumericDocValuesField("intSortfieldA", 2));
        doc4.add(new NumericDocValuesField("doubleSortfieldA", NumericUtils.doubleToSortableLong(2.0)));
        luceneA.addDocument("id4", doc4);
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
        assertEquals(2, response.total);
        assertEquals("id3", response.hits.get(0).id);
        assertEquals("id4", response.hits.get(1).id);
        
        q.queryData.sort = new Sort(new JoinSortField("sortfieldA", SortField.Type.STRING, true, "A"));
        response = multiLucene.executeComposedQuery(q);
        assertEquals(2, response.total);
        assertEquals("id4", response.hits.get(0).id);
        assertEquals("id3", response.hits.get(1).id);
    }

    @Test
    public void testJoinSortStringField() throws Throwable {
        ComposedQuery q = new ComposedQuery("A");
        q.addMatch("A", "B", "keyA", "keyB");
        q.setCoreQuery("A", new MatchAllDocsQuery());
        q.queryData.sort = new Sort(new JoinSortField("sortfieldB", SortField.Type.STRING, false, "B"));
        LuceneResponse response = multiLucene.executeComposedQuery(q);
        assertEquals(2, response.total);
        assertEquals("id3", response.hits.get(0).id);
        assertEquals("id4", response.hits.get(1).id);
        
        q.queryData.sort = new Sort(new JoinSortField("sortfieldB", SortField.Type.STRING, true, "B"));
        response = multiLucene.executeComposedQuery(q);
        assertEquals(2, response.total);
        assertEquals("id4", response.hits.get(0).id);
        assertEquals("id3", response.hits.get(1).id);
    }
    
    @Test
    public void testJoinSortIntField() throws Throwable {
        ComposedQuery q = new ComposedQuery("A");
        q.addMatch("A", "B", "keyA", "keyB");
        q.setCoreQuery("A", new MatchAllDocsQuery());
        q.queryData.sort = new Sort(new JoinSortField("intSortfieldB", SortField.Type.INT, false, "B"));
        LuceneResponse response = multiLucene.executeComposedQuery(q);
        assertEquals(2, response.total);
        assertEquals("id3", response.hits.get(0).id);
        assertEquals("id4", response.hits.get(1).id);
        
        q.queryData.sort = new Sort(new JoinSortField("intSortfieldB", SortField.Type.INT, true, "B"));
        response = multiLucene.executeComposedQuery(q);
        assertEquals(2, response.total);
        assertEquals("id4", response.hits.get(0).id);
        assertEquals("id3", response.hits.get(1).id);
    }
    
    @Test
    public void testJoinSortDoubleField() throws Throwable {
        ComposedQuery q = new ComposedQuery("A");
        q.addMatch("A", "B", "keyA", "keyB");
        q.setCoreQuery("A", new MatchAllDocsQuery());
        q.queryData.sort = new Sort(new JoinSortField("doubleSortfieldB", SortField.Type.DOUBLE, false, "B"));
        LuceneResponse response = multiLucene.executeComposedQuery(q);
        assertEquals(2, response.total);
        assertEquals("id3", response.hits.get(0).id);
        assertEquals("id4", response.hits.get(1).id);
        
        q.queryData.sort = new Sort(new JoinSortField("doubleSortfieldB", SortField.Type.DOUBLE, true, "B"));
        response = multiLucene.executeComposedQuery(q);
        assertEquals(2, response.total);
        assertEquals("id4", response.hits.get(0).id);
        assertEquals("id3", response.hits.get(1).id);
    }
}

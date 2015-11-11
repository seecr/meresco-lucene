package test;

import static org.junit.Assert.assertEquals;

import java.io.File;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.meresco.lucene.Lucene;
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
        LuceneResponse response = lucene.executeQuery(new MatchAllDocsQuery(), 0, 10, null);
        assertEquals(1, response.total);
        assertEquals(1, response.hits.size());
        assertEquals("id1", response.hits.get(0).id);
    }

}

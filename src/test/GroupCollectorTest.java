package test;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreDoc;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.meresco.lucene.Lucene;
import org.meresco.lucene.LuceneSettings;
import org.meresco.lucene.search.GroupSuperCollector;
import org.meresco.lucene.search.TopScoreDocSuperCollector;

public class GroupCollectorTest extends SeecrTestCase {

    private Lucene lucene;
    
    @Before
    public void setUp() throws Exception {
        super.setUp();
        LuceneSettings settings = new LuceneSettings();
        settings.commitCount = 1;
        lucene = new Lucene(this.tmpDir, settings);
    }
    
    @After
    public void tearDown() throws Exception {
        lucene.close();
        super.tearDown();
    }
    
    @Test
    public void test() throws Exception {
        addDocument("id:0", 42L);
        addDocument("id:1", 42L);
        addDocument("id:2", 42L);
        addDocument("id:3", 17L);
        TopScoreDocSuperCollector tc = new TopScoreDocSuperCollector(100, true);
        GroupSuperCollector c = new GroupSuperCollector("__isformatof__", tc);
        lucene.search(new MatchAllDocsQuery(), c);
        assertEquals(4, tc.getTotalHits());
        Map<String, Integer> idFields = new HashMap<String, Integer>();
        for (ScoreDoc scoreDoc : tc.topDocs(0).scoreDocs) {
            idFields.put(lucene.getDocument(scoreDoc.doc).get(Lucene.ID_FIELD), scoreDoc.doc);
        }
        
        assertEquals(3, c.group(idFields.get("id:0")).size());
        assertEquals(3, c.group(idFields.get("id:1")).size());
        assertEquals(3, c.group(idFields.get("id:2")).size());
        assertEquals(1, c.group(idFields.get("id:3")).size());
    }

    public void addDocument(String identifier, Long isformatof) throws IOException {
        Document doc = new Document();
        if (isformatof != null)
            doc.add(new NumericDocValuesField("__isformatof__", isformatof));
        lucene.addDocument(identifier, doc);
        lucene.commit();
    }
}

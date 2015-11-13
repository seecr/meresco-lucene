package test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;

import java.io.IOException;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexReader;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.meresco.lucene.Lucene;
import org.meresco.lucene.LuceneSettings;
import org.meresco.lucene.search.MerescoCluster;
import org.meresco.lucene.search.MerescoClusterer;

public class MerescoClustererTest extends SeecrTestCase {

    private Lucene lucene;
    private MerescoClusterer collector;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        Document doc;

        lucene = new Lucene(this.tmpDir, new LuceneSettings());
        FieldType fieldType = new FieldType(TextField.TYPE_NOT_STORED);
        fieldType.setStoreTermVectors(true);

        for (int i = 0; i < 5; i++) {
            doc = new Document();
            doc.add(new Field("termvector.field", "aap noot noot noot vuur", fieldType));
            lucene.addDocument("id:" + i, doc);
        }

        for (int i = 5; i < 10; i++) {
            doc = new Document();
            doc.add(new Field("termvector.field", "something else", fieldType));
            lucene.addDocument("id:" + i, doc);
        }
        for (int i = 10; i < 15; i++) {
            doc = new Document();
            doc.add(new Field("termvector.field", "iets anders", fieldType));
            lucene.addDocument("id:" + i, doc);
        }
        lucene.commit();
        IndexReader reader = lucene.indexAndTaxo.searcher().getIndexReader();
        collector = new MerescoClusterer(reader, 0.5);
    }
    
    @After
    public void tearDown() throws Exception {
        lucene.close();
        super.tearDown();
    }

    @Test
    public void testClusterOnTermVectors() throws IOException {
        collector.registerField("termvector.field", 1.0, null);
        for (int i = 0; i < 15; i++) {
            collector.collect(i);
        }
        collector.finish();

        MerescoCluster cluster1 = collector.cluster(0);
        assertEquals(5, cluster1.topDocs.length);
        assertEquals(2, cluster1.topTerms.length);
        String[] terms = new String[cluster1.topTerms.length];
        for (int i = 0; i < terms.length; i++) {
            terms[i] = cluster1.topTerms[i].term;
        }
        assertArrayEquals(new String[] { "else", "something" }, terms);

        MerescoCluster cluster2 = collector.cluster(5);
        assertEquals(5, cluster2.topDocs.length);
        terms = new String[cluster2.topTerms.length];
        for (int i = 0; i < terms.length; i++) {
            terms[i] = cluster2.topTerms[i].term;
        }
        assertArrayEquals(new String[] { "noot", "aap", "vuur" }, terms);

        MerescoCluster cluster3 = collector.cluster(10);
        assertEquals(5, cluster3.topDocs.length);
        terms = new String[cluster3.topTerms.length];
        for (int i = 0; i < terms.length; i++) {
            terms[i] = cluster3.topTerms[i].term;
        }
        assertArrayEquals(new String[] { "anders", "iets" }, terms);

        assertNotSame(cluster1.topDocs, cluster2.topDocs);
        assertNotSame(cluster1.topDocs, cluster3.topDocs);
    }

    @Test
    public void testClusteringWithFieldFilter() throws IOException {
        collector.registerField("termvector.field", 1.0, "noot");
        for (int i = 0; i < 15; i++) {
            collector.collect(i);
        }
        collector.finish();

        MerescoCluster cluster1 = collector.cluster(0);
        assertEquals(null, cluster1);

        MerescoCluster cluster2 = collector.cluster(5);
        assertEquals(5, cluster2.topDocs.length);
        String[] terms = new String[cluster2.topTerms.length];
        for (int i = 0; i < terms.length; i++) {
            terms[i] = cluster2.topTerms[i].term;
        }
        assertArrayEquals(new String[] { "noot", "aap", "vuur" }, terms);

        MerescoCluster cluster3 = collector.cluster(10);
        assertEquals(null, cluster3);
    }
}

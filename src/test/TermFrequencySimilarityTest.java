package test;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.junit.Before;
import org.junit.Test;
import org.meresco.lucene.Lucene;
import org.meresco.lucene.LuceneResponse;
import org.meresco.lucene.LuceneSettings;
import org.meresco.lucene.search.TermFrequencySimilarity;

public class TermFrequencySimilarityTest extends SeecrTestCase {

    @Test
    public void test() throws Exception {
        LuceneSettings settings = new LuceneSettings();
        settings.commitCount = 1;
        settings.similarity = new TermFrequencySimilarity();
        Lucene lucene = new Lucene(this.tmpDir, settings);
        Document document = new Document();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 100; i++) {
            sb.append("x ");    
        }
        document.add(new TextField("field", sb.toString(), Store.NO));
        lucene.addDocument("identifier", document);

        Query q = new TermQuery(new Term("field", "x"));
        LuceneResponse result = lucene.executeQuery(q);
        assertEquals(0.1, result.hits.get(0).score, 0.0002);

        q.setBoost((float) 10.0);
        result = lucene.executeQuery(q);
        assertEquals(1, result.hits.get(0).score, 0.0002);
    }

}

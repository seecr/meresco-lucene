package org.meresco.lucene;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

import java.io.StringReader;
import java.util.ArrayList;

import org.apache.lucene.facet.FacetsConfig.DimConfig;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.junit.Test;
import org.meresco.lucene.analysis.MerescoDutchStemmingAnalyzer;
import org.meresco.lucene.analysis.MerescoStandardAnalyzer;
import org.meresco.lucene.http.SettingsHandler;

public class UpdateSettingsTest {

    @Test
    public void test() throws Exception {
        LuceneSettings settings = new LuceneSettings();
        String json = "{\"commitCount\": 1, \"commitTimeout\": 1, \"lruTaxonomyWriterCacheSize\": 1, \"maxMergeAtOnce\": 1, \"segmentsPerTier\": 1.0, \"numberOfConcurrentTasks\": 1}";
        
        SettingsHandler.updateSettings(settings, new StringReader(json));
        assertEquals(1, settings.commitCount);
        assertEquals(1, settings.commitTimeout);
        assertEquals(1, settings.lruTaxonomyWriterCacheSize);
        assertEquals(1, settings.maxMergeAtOnce);
        assertEquals(1.0, settings.segmentsPerTier, 0);
        assertEquals(1, settings.commitTimeout);
    }
    
    @Test
    public void testMerescoDutchStemmingAnalyzer() throws Exception {
        LuceneSettings settings = new LuceneSettings();
        String json = "{\"analyzer\": {\"type\": \"MerescoDutchStemmingAnalyzer\", \"fields\": [\"field0\", \"field1\"]}}";
        
        SettingsHandler.updateSettings(settings, new StringReader(json));
        assertEquals(MerescoDutchStemmingAnalyzer.class, settings.analyzer.getClass());
        
        assertEquals(new ArrayList<String>() {{ add("katten"); add("kat");}}, MerescoStandardAnalyzer.readTokenStream(settings.analyzer.tokenStream("field0", "katten")));
        assertEquals(new ArrayList<String>() {{ add("katten"); add("kat");}}, MerescoStandardAnalyzer.readTokenStream(settings.analyzer.tokenStream("field1", "katten")));
        assertEquals(new ArrayList<String>() {{ add("katten");}}, MerescoStandardAnalyzer.readTokenStream(settings.analyzer.tokenStream("field2", "katten")));
    }
    
    @Test
    public void testMerescoStandardAnalyzer() throws Exception {
        LuceneSettings settings = new LuceneSettings();
        String json = "{\"analyzer\": {\"type\": \"MerescoStandardAnalyzer\", \"fields\": [\"field0\", \"field1\"]}}";
        
        SettingsHandler.updateSettings(settings, new StringReader(json));
        assertEquals(MerescoStandardAnalyzer.class, settings.analyzer.getClass());
    }

    @Test
    public void testBM25Similarity() throws Exception {
        LuceneSettings settings = new LuceneSettings();
        String json = "{\"analyzer\": {\"type\": \"BM25Similarity\"}}";
        
        SettingsHandler.updateSettings(settings, new StringReader(json));
        assertEquals(BM25Similarity.class, settings.similarity.getClass());
        assertEquals(0.75f, ((BM25Similarity) settings.similarity).getB(), 0);
        assertEquals(1.2f, ((BM25Similarity) settings.similarity).getK1(), 0);
    }
    
    @Test
    public void testBM25SimilarityWithKAndB() throws Exception {
        LuceneSettings settings = new LuceneSettings();
        String json = "{\"similarity\": {\"type\": \"BM25Similarity\", \"k1\": 1.0, \"b\": 2.0}}";
        
        SettingsHandler.updateSettings(settings, new StringReader(json));
        assertEquals(BM25Similarity.class, settings.similarity.getClass());
        assertEquals(2.0f, ((BM25Similarity) settings.similarity).getB(), 0);
        assertEquals(1.0f, ((BM25Similarity) settings.similarity).getK1(), 0);
    }
    
    @Test
    public void testDrilldownFields() throws Exception {
        LuceneSettings settings = new LuceneSettings();
        String json = "{\"drilldownFields\": [" +
        		    "{\"dim\": \"field0\", \"hierarchical\": true, \"multiValued\": false}," +
        		    "{\"dim\": \"field1\", \"hierarchical\": true, \"multiValued\": true, \"fieldname\": \"$facets_2\"}" +
    		    "]}";
        
        SettingsHandler.updateSettings(settings, new StringReader(json));
        DimConfig field0 = settings.facetsConfig.getDimConfig("field0");
        assertTrue(field0.hierarchical);
        assertFalse(field0.multiValued);
        assertEquals("$facets", field0.indexFieldName);
        
        DimConfig field1 = settings.facetsConfig.getDimConfig("field1");
        assertTrue(field1.hierarchical);
        assertTrue(field1.multiValued);
        assertEquals("$facets_2", field1.indexFieldName);
    }
}

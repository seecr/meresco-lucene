package org.meresco.lucene;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.Similarity;
import org.meresco.lucene.analysis.MerescoStandardAnalyzer;

public class LuceneSettings {

    public Similarity similarity = new BM25Similarity();
    public Analyzer analyzer = new MerescoStandardAnalyzer();
    public int maxMergeAtOnce = 2;
    public double segmentsPerTier = 8.0;
    public int lruTaxonomyWriterCacheSize = 4000;
    public int numberOfConcurrentTasks = 6;
    public int commitTimeout = 10;
    public int commitCount = 100000;
    public FacetsConfig facetsConfig = new FacetsConfig();
}

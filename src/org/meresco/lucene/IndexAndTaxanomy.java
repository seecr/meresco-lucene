package org.meresco.lucene;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.Directory;
import org.meresco.lucene.search.SuperIndexSearcher;

public class IndexAndTaxanomy {
    private int numberOfConcurrentTasks;
    DirectoryReader reader;
    DirectoryTaxonomyReader taxoReader;
    private ExecutorService executor = null;
    private SuperIndexSearcher searcher;
    private boolean reopenSearcher = true;
    private LuceneSettings settings;
    
    public IndexAndTaxanomy(Directory indexDirectory, Directory taxoDirectory, LuceneSettings settings) throws IOException {
        this.numberOfConcurrentTasks = settings.numberOfConcurrentTasks;
        this.reader = DirectoryReader.open(indexDirectory);
        this.taxoReader = new DirectoryTaxonomyReader(taxoDirectory);
        this.settings = settings;
    }
    
    public boolean reopen() throws IOException {
        DirectoryReader reader = DirectoryReader.openIfChanged(this.reader);
        if (reader == null)
            return false;
        this.reader.close();
        this.reader = reader;
        this.reopenSearcher = true;
        DirectoryTaxonomyReader taxoReader = DirectoryTaxonomyReader.openIfChanged(this.taxoReader);
        if (taxoReader == null)
            return true;
        this.taxoReader.close();
        this.taxoReader = taxoReader;
        return true;
    }
    
    public SuperIndexSearcher searcher() { 
        if (!this.reopenSearcher)
            return this.searcher;
        
        if (this.executor != null)
            this.executor.shutdown();
        this.executor  = Executors.newFixedThreadPool(this.numberOfConcurrentTasks);
        this.searcher = new SuperIndexSearcher(this.reader, this.executor, this.numberOfConcurrentTasks);
        this.searcher.setSimilarity(this.settings.similarity);
        return this.searcher;
    }
    
    public void close() throws IOException {
        this.taxoReader.close();
        this.reader.close();
    }
}

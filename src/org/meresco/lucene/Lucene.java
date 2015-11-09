package org.meresco.lucene;

import java.io.File;
import java.io.IOException;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.facet.taxonomy.writercache.LruTaxonomyWriterCache;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.Version;

public class Lucene {

    private IndexWriter indexWriter;
    private DirectoryTaxonomyWriter taxoWriter;
    private DirectoryReader reader;

    public Lucene(File stateDir) throws IOException {
        MMapDirectory indexDirectory = new MMapDirectory(new File(stateDir, "index"));
        indexDirectory.setUseUnmap(false);

        MMapDirectory taxoDirectory = new MMapDirectory(new File(stateDir, "taxo"));
        taxoDirectory.setUseUnmap(false);

        IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_4_10_4, new StandardAnalyzer());
        
        indexWriter = new IndexWriter(indexDirectory, config);
        indexWriter.commit();
        taxoWriter = new DirectoryTaxonomyWriter(taxoDirectory, IndexWriterConfig.OpenMode.CREATE_OR_APPEND, new LruTaxonomyWriterCache(4000));
        taxoWriter.commit();
        
        reader = DirectoryReader.open(indexDirectory);
    }
    
    public void close() throws IOException {
        reader.close();
        taxoWriter.close();
        indexWriter.close();
    }
}

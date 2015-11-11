package org.meresco.lucene;

import java.io.File;
import java.io.IOException;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.facet.taxonomy.writercache.LruTaxonomyWriterCache;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.Version;
import org.meresco.lucene.search.TopDocSuperCollector;
import org.meresco.lucene.search.TopFieldSuperCollector;
import org.meresco.lucene.search.TopScoreDocSuperCollector;

public class Lucene {

    private static final String ID_FIELD = "__id__";
    private IndexWriter indexWriter;
    private DirectoryTaxonomyWriter taxoWriter;
    private IndexAndTaxanomy indexAndTaxo;

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
        
        indexAndTaxo = new IndexAndTaxanomy(indexDirectory, taxoDirectory, 6);
    }
    
    public void close() throws IOException {
        indexAndTaxo.close();
        taxoWriter.close();
        indexWriter.close();
    }

    public void addDocument(String identifier, Document doc) throws IOException {
        doc.add(new StringField(ID_FIELD, identifier, Store.YES));
        indexWriter.updateDocument(new Term(ID_FIELD, identifier), doc);
        commit();
    }
    
    public void commit() throws IOException {
        indexWriter.commit();
        indexAndTaxo.reopen();
    }
    
    public LuceneResponse executeQuery(Query query, int start, int stop, String[] sortKeys) throws Exception {
        TopDocSuperCollector topCollector = createCollectors(start, stop, sortKeys);
        
        indexAndTaxo.searcher().search(query, null, topCollector);
        LuceneResponse response = new LuceneResponse(topCollector.getTotalHits());
        for (ScoreDoc scoreDoc : topCollector.topDocs(start).scoreDocs) {
            response.addHit(getDocument(scoreDoc.doc).get(ID_FIELD));
        }
        return response;
    }
    
    private Document getDocument(int docID) throws IOException {
        return indexAndTaxo.searcher().doc(docID);
    }

    private TopDocSuperCollector createCollectors(int start, int stop, String[] sortKeys) {
        return topCollector(start, stop, sortKeys);
    }
    
    private TopDocSuperCollector topCollector(int start, int stop, String[] sortKeys) {
//        if (stop <= start)
//            return new TotalHitCountSuperCollector();
        Sort sort = null;
        if (sortKeys != null) {
            SortField[] sortFields = new SortField[sortKeys.length];
            for (int i=0; i<sortKeys.length; i++) {
                
            }
//            for sortKey in sortKeys:
//                if isinstance(sortKey, dict):
//                    sortFields.append(self._sortField(sortKey))
//                else:
//                    sortFields.append(sortKey)
            sort = new Sort(sortFields);
        } else
            return new TopScoreDocSuperCollector(stop, true);
        return new TopFieldSuperCollector(sort, stop, true, false, true);
    }
}

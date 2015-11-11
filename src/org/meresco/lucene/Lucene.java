package org.meresco.lucene;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.LabelAndValue;
import org.apache.lucene.facet.taxonomy.CachedOrdinalsReader;
import org.apache.lucene.facet.taxonomy.DocValuesOrdinalsReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.facet.taxonomy.writercache.LruTaxonomyWriterCache;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.Version;
import org.meresco.lucene.LuceneResponse.DrilldownData;
import org.meresco.lucene.QueryStringToQuery.FacetRequest;
import org.meresco.lucene.search.FacetSuperCollector;
import org.meresco.lucene.search.MultiSuperCollector;
import org.meresco.lucene.search.SuperCollector;
import org.meresco.lucene.search.TopDocSuperCollector;
import org.meresco.lucene.search.TopFieldSuperCollector;
import org.meresco.lucene.search.TopScoreDocSuperCollector;

public class Lucene {

    private static final String ID_FIELD = "__id__";
    private IndexWriter indexWriter;
    private DirectoryTaxonomyWriter taxoWriter;
    private IndexAndTaxanomy indexAndTaxo;
    private FacetsConfig facetsConfig;

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
        facetsConfig = new FacetsConfig();
    }
    
    public void close() throws IOException {
        indexAndTaxo.close();
        taxoWriter.close();
        indexWriter.close();
    }

    public void addDocument(String identifier, Document doc) throws IOException {
        doc.add(new StringField(ID_FIELD, identifier, Store.YES));
        doc = facetsConfig.build(taxoWriter, doc);
        indexWriter.updateDocument(new Term(ID_FIELD, identifier), doc);
        commit();
    }
    
    public void commit() throws IOException {
        indexWriter.commit();
        taxoWriter.commit();
        indexAndTaxo.reopen();
    }
    
    public LuceneResponse executeQuery(Query query) throws Exception {
        return executeQuery(query, 0, 10, null, null);
    }
    
    public LuceneResponse executeQuery(Query query, List<FacetRequest> facets) throws Exception {
        return executeQuery(query, 0, 10, null, facets);
    }
    
    public LuceneResponse executeQuery(Query query, int start, int stop, String[] sortKeys, List<FacetRequest> facets) throws Exception {
        Collectors collectors = createCollectors(start, stop, sortKeys, facets);
        
        indexAndTaxo.searcher().search(query, null, collectors.root);
        LuceneResponse response = new LuceneResponse(collectors.topCollector.getTotalHits());
        for (ScoreDoc scoreDoc : collectors.topCollector.topDocs(start).scoreDocs) {
            response.addHit(getDocument(scoreDoc.doc).get(ID_FIELD));
        }
        if (collectors.facetCollector != null)
            facetResult(response, collectors.facetCollector, facets);
        return response;
    }

    private Document getDocument(int docID) throws IOException {
        return indexAndTaxo.searcher().doc(docID);
    }

    private Collectors createCollectors(int start, int stop, String[] sortKeys, List<FacetRequest> facets) {
        Collectors allCollectors = new Collectors();
        allCollectors.topCollector = topCollector(start, stop, sortKeys);
        allCollectors.facetCollector = facetCollector(facets);
        
        List<SuperCollector<?>> collectors = new ArrayList<SuperCollector<?>>();
        collectors.add(allCollectors.topCollector);
        if (allCollectors.facetCollector != null) {
            collectors.add(allCollectors.facetCollector);
        }
        allCollectors.root = new MultiSuperCollector(collectors);
        return allCollectors;
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
    
    private FacetSuperCollector facetCollector(List<FacetRequest> facets) {
        if (facets == null)
            return null;
        return new FacetSuperCollector(indexAndTaxo.taxoReader, facetsConfig, new CachedOrdinalsReader(new DocValuesOrdinalsReader()));
    }
    
    private void facetResult(LuceneResponse response, FacetSuperCollector facetCollector, List<FacetRequest> facets) throws IOException {
        List<DrilldownData> drilldownData = new ArrayList<DrilldownData>();
        for (FacetRequest facet : facets) {
            DrilldownData dd = new DrilldownData(facet.fieldname);
            
            FacetResult result = facetCollector.getTopChildren(facet.maxTerms, facet.fieldname, facet.path);
            if (result == null)
                continue;
            for (LabelAndValue l : result.labelValues) {
                dd.addTerm(l.label, l.value.intValue());
            }
            drilldownData.add(dd);
        }
        response.drilldownData = drilldownData;
    }
    
    public static class Collectors {
        public TopDocSuperCollector topCollector;
        public FacetSuperCollector facetCollector;
        public SuperCollector<?> root;
    }
}

/* begin license *
 *
 * "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
 *
 * Copyright (C) 2015 Koninklijke Bibliotheek (KB) http://www.kb.nl
 * Copyright (C) 2015 Seecr (Seek You Too B.V.) http://seecr.nl
 *
 * This file is part of "Meresco Lucene"
 *
 * "Meresco Lucene" is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * "Meresco Lucene" is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with "Meresco Lucene"; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
 *
 * end license */

package org.meresco.lucene;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.facet.DrillDownQuery;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.LabelAndValue;
import org.apache.lucene.facet.taxonomy.CachedOrdinalsReader;
import org.apache.lucene.facet.taxonomy.DocValuesOrdinalsReader;
import org.apache.lucene.facet.taxonomy.OrdinalsReader;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.taxonomy.TaxonomyReader.ChildrenIterator;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.facet.taxonomy.writercache.LruTaxonomyWriterCache;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.queries.ChainedFilter;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.CachingWrapperFilter;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryWrapperFilter;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.OpenBitSet;
import org.apache.lucene.util.Version;
import org.meresco.lucene.LuceneResponse.DrilldownData;
import org.meresco.lucene.QueryConverter.FacetRequest;
import org.meresco.lucene.search.FacetSuperCollector;
import org.meresco.lucene.search.MultiSuperCollector;
import org.meresco.lucene.search.SuperCollector;
import org.meresco.lucene.search.TopDocSuperCollector;
import org.meresco.lucene.search.TopFieldSuperCollector;
import org.meresco.lucene.search.TopScoreDocSuperCollector;
import org.meresco.lucene.search.join.KeySuperCollector;

public class Lucene {

    public static final String ID_FIELD = "__id__";
    IndexWriter indexWriter;
    DirectoryTaxonomyWriter taxoWriter;
    IndexAndTaxanomy indexAndTaxo;
    FacetsConfig facetsConfig;
    private LuceneSettings settings;
    private int commitCount = 0;
    private Timer commitTimer;
    public String name;
    private File stateDir;
    private Map<String, CachedOrdinalsReader> cachedOrdinalsReader = new HashMap<String, CachedOrdinalsReader>();

    public Lucene(String name, File stateDir) {
        this.name = name;
        this.stateDir = stateDir;
    }

    public Lucene(File stateDir, LuceneSettings settings) throws Exception {
        this(null, stateDir, settings);
    }

    public Lucene(String name, File stateDir, LuceneSettings settings) throws Exception {
        this.name = name;
        this.stateDir = stateDir;
        initSettings(settings);
    }

    public void initSettings(LuceneSettings settings) throws Exception {
        if (this.settings != null)
            throw new Exception("Init settings is only allowed once");
        this.settings = settings;

        MMapDirectory indexDirectory = new MMapDirectory(new File(stateDir, "index"));
        indexDirectory.setUseUnmap(false);

        MMapDirectory taxoDirectory = new MMapDirectory(new File(stateDir, "taxo"));
        taxoDirectory.setUseUnmap(false);

        IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_4_10_4, settings.analyzer);
        config.setSimilarity(settings.similarity);
        TieredMergePolicy mergePolicy = new TieredMergePolicy();
        mergePolicy.setMaxMergeAtOnce(settings.maxMergeAtOnce);
        mergePolicy.setSegmentsPerTier(settings.segmentsPerTier);
        config.setMergePolicy(mergePolicy);

        indexWriter = new IndexWriter(indexDirectory, config);
        indexWriter.commit();
        taxoWriter = new DirectoryTaxonomyWriter(taxoDirectory, IndexWriterConfig.OpenMode.CREATE_OR_APPEND, new LruTaxonomyWriterCache(settings.lruTaxonomyWriterCacheSize));
        taxoWriter.commit();

        indexAndTaxo = new IndexAndTaxanomy(indexDirectory, taxoDirectory, settings);
        facetsConfig = settings.facetsConfig;
    }

    public LuceneSettings getSettings() {
        return this.settings;
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

    public void deleteDocument(String identifier) throws IOException {
        indexWriter.deleteDocuments(new Term(ID_FIELD, identifier));
        commit();
    }

    public void commit() throws IOException {
        commitCount++;
        if (commitCount >= settings.commitCount) {
            realCommit();
            return;
        }
        if (commitTimer == null) {
            TimerTask timerTask = new TimerTask() {
                public void run() {
                    try {
                        realCommit();
                    } catch (IOException e) {
                        throw new RuntimeException();
                    }
                }
            };
            commitTimer = new Timer();
            commitTimer.schedule(timerTask, settings.commitTimeout * 1000);
        }
    }

    public synchronized void realCommit() throws IOException {
        commitCount = 0;
        if (commitTimer != null) {
            commitTimer.cancel();
            commitTimer.purge();
            commitTimer = null;
        }
        indexWriter.commit();
        taxoWriter.commit();
        indexAndTaxo.reopen();
    }

    public LuceneResponse executeQuery(Query query) throws Exception {
        return executeQuery(query, 0, 10, null, null, null, null, null);
    }

    public LuceneResponse executeQuery(Query query, List<FacetRequest> facets) throws Exception {
        return executeQuery(query, 0, 10, null, facets, null, null, null);
    }

    public LuceneResponse executeQuery(Query query, int start, int stop, Sort sort, List<FacetRequest> facets, List<Filter> filters, Collection<KeySuperCollector> keyCollectors, Map<String, String> drilldownQueries) throws Exception {
        long t0 = System.currentTimeMillis();
        Collectors collectors = createCollectors(start, stop, sort, facets, keyCollectors);

        Filter f = filtersFor(null, filters == null ? null : filters.toArray(new Filter[0]));

        if (drilldownQueries != null) 
            query = createDrilldownQuery(query, drilldownQueries);
        indexAndTaxo.searcher().search(query, f, collectors.root);
        LuceneResponse response = new LuceneResponse(collectors.topCollector.getTotalHits());
        for (ScoreDoc scoreDoc : collectors.topCollector.topDocs(stop == 0 ? 1 : start).scoreDocs) { //TODO: temp fix for start/stop = 0
            response.addHit(getDocument(scoreDoc.doc).get(ID_FIELD), scoreDoc.score);
        }
        if (collectors.facetCollector != null)
            response.drilldownData = facetResult(collectors.facetCollector, facets);

        response.queryTime = System.currentTimeMillis() - t0;
        return response;
    }

    public List<DrilldownData> facets(List<FacetRequest> facets, List<Query> filterQueries, Map<String, String> drilldownQueries, Filter filter) throws Exception {
        FacetSuperCollector facetCollector = facetCollector(facets);
        if (facetCollector == null)
            return new ArrayList<DrilldownData>();
        Filter filter_ = filtersFor(filterQueries, filter);
        Query query = new MatchAllDocsQuery();
        if (drilldownQueries != null)
            query = createDrilldownQuery(query, drilldownQueries);
        indexAndTaxo.searcher().search(query, filter_, facetCollector);
        return facetResult(facetCollector, facets);
    }

    private Filter filtersFor(List<Query> filterQueries, Filter... filter) {
        List<Filter> filters = new ArrayList<Filter>();
        if (filterQueries != null)
            for (Query query : filterQueries)
                filters.add(new CachingWrapperFilter(new QueryWrapperFilter(query))); //TODO: Use filterCache
        if (filter != null)
            for (Filter f : filter)
                if (f != null)
                    filters.add(f);
        if (filters.size() == 0)
            return null;
        return new ChainedFilter(filters.toArray(new Filter[0]), ChainedFilter.AND);
    }

    public Document getDocument(int docID) throws IOException {
        return indexAndTaxo.searcher().doc(docID);
    }

    private Collectors createCollectors(int start, int stop, Sort sort, List<FacetRequest> facets, Collection<KeySuperCollector> keyCollectors) {
        Collectors allCollectors = new Collectors();
        allCollectors.topCollector = topCollector(start, stop, sort);
        allCollectors.facetCollector = facetCollector(facets);

        List<SuperCollector<?>> collectors = new ArrayList<SuperCollector<?>>();
        collectors.add(allCollectors.topCollector);
        if (allCollectors.facetCollector != null) {
            collectors.add(allCollectors.facetCollector);
        }
        if (keyCollectors != null)
            collectors.addAll(keyCollectors);
        allCollectors.root = new MultiSuperCollector(collectors);
        return allCollectors;
    }

    private TopDocSuperCollector topCollector(int start, int stop, Sort sort) {
        if (stop <= start)
            //TODO: temp fix for start/stop = 0; You should use TotalHitCountSuperCollector
            return new TopScoreDocSuperCollector(stop == 0 ? 1 : stop, true);
//            return new TotalHitCountSuperCollector();
        if (sort == null)
            return new TopScoreDocSuperCollector(stop, true);
        return new TopFieldSuperCollector(sort, stop, true, false, true);
    }

    private FacetSuperCollector facetCollector(List<FacetRequest> facets) {
        if (facets == null || facets.size() == 0)
            return null;
        String[] indexFieldnames = getIndexFieldNames(facets);
        FacetSuperCollector collector = new FacetSuperCollector(indexAndTaxo.taxoReader, facetsConfig, getOrdinalsReader(indexFieldnames[0]));
        for (int i = 1; i < indexFieldnames.length; i++) {
            collector.addOrdinalsReader(getOrdinalsReader(indexFieldnames[i]));
        }
        return collector;
    }

    String[] getIndexFieldNames(List<FacetRequest> facets) {
        Set<String> indexFieldnames = new HashSet<String>();
        for (FacetRequest f : facets)
            indexFieldnames.add(this.facetsConfig.getDimConfig(f.fieldname).indexFieldName);
        return indexFieldnames.toArray(new String[0]);
    }

    private OrdinalsReader getOrdinalsReader(String indexFieldname) {
        CachedOrdinalsReader reader = cachedOrdinalsReader.get(indexFieldname);
        if (reader == null) {
            DocValuesOrdinalsReader docValuesReader = indexFieldname == null ? new DocValuesOrdinalsReader() : new DocValuesOrdinalsReader(indexFieldname);
            reader = new CachedOrdinalsReader(docValuesReader);
            cachedOrdinalsReader.put(indexFieldname, reader);
        }
        return reader;
    }

    private List<DrilldownData> facetResult(FacetSuperCollector facetCollector, List<FacetRequest> facets) throws IOException {
        List<DrilldownData> drilldownData = new ArrayList<DrilldownData>();
        for (FacetRequest facet : facets) {
            DrilldownData dd = new DrilldownData(facet.fieldname);

            FacetResult result = facetCollector.getTopChildren(facet.maxTerms == 0 ? Integer.MAX_VALUE : facet.maxTerms, facet.fieldname, facet.path);
            if (result == null)
                continue;
            for (LabelAndValue l : result.labelValues) {
                dd.addTerm(l);
            }
            drilldownData.add(dd);
        }
        return drilldownData;
    }

    public List<TermCount> termsForField(String field, String prefix, int limit) throws Exception {

//        if t == str:
//            convert = lambda term: term.utf8ToString()
//        elif t == int:
//            convert = lambda term: NumericUtils.prefixCodedToInt(term)
//        elif t == long:
//            convert = lambda term: NumericUtils.prefixCodedToLong(term)
//        elif t == float:
//            convert = lambda term: NumericUtils.sortableLongToDouble(NumericUtils.prefixCodedToLong(term))

        List<TermCount> terms = new ArrayList<TermCount>();
        Terms termsEnum = MultiFields.getTerms(this.indexAndTaxo.reader, field);
        if (termsEnum == null)
            return terms;
        TermsEnum iterator = termsEnum.iterator(null);
        if (prefix != null) {
            iterator.seekCeil(new BytesRef(prefix));
            terms.add(new TermCount(iterator.term().utf8ToString(), iterator.docFreq()));
        }
        while (terms.size() < limit) {
            BytesRef next = iterator.next();
            if (next == null)
                break;
            String term = next.utf8ToString();
            if (prefix != null && !term.startsWith(prefix)) {
                break;
            }
            terms.add(new TermCount(term, iterator.docFreq()));
        }
        return terms;
    }

    public static class Collectors {
        public TopDocSuperCollector topCollector;
        public FacetSuperCollector facetCollector;
        public SuperCollector<?> root;
    }

    public class TermCount {
        public String term;
        public int count;
        public TermCount(String term, int count) {
            this.term = term;
            this.count = count;
        }
    }

    public int numDocs() {
        return this.indexWriter.numDocs();
    }

    public int maxDoc() {
        return this.indexWriter.maxDoc();
    }

    public List<String> fieldnames() throws IOException {
        List<String> fieldnames = new ArrayList<String>();
        Fields fields = MultiFields.getFields(this.indexAndTaxo.reader);
        if (fields == null)
            return fieldnames;
        for (Iterator<String> iterator = fields.iterator(); iterator.hasNext();) {
            fieldnames.add(iterator.next());
        }
        return fieldnames;
    }

    public List<String> drilldownFieldnames(int limit, String dim, String... path) throws IOException {
        DirectoryTaxonomyReader taxoReader = this.indexAndTaxo.taxoReader;
        int parentOrdinal = dim == null ? TaxonomyReader.ROOT_ORDINAL : taxoReader.getOrdinal(dim, path);
        ChildrenIterator childrenIter = taxoReader.getChildren(parentOrdinal);
        List<String> fieldnames = new ArrayList<String>();
        while (true) {
            int ordinal = childrenIter.next();
            if (ordinal == TaxonomyReader.INVALID_ORDINAL)
                break;
            String[] components = taxoReader.getPath(ordinal).components;
            fieldnames.add(components[components.length - 1 ]);
            if (fieldnames.size() >= limit)
                break;
        }
        return fieldnames;
    }

    public void search(Query query, Query filterQuery, SuperCollector<?> collector) throws Exception {
        Filter filter_ = null;
        if (filterQuery != null)
            filter_ = new QueryWrapperFilter(filterQuery);
        indexAndTaxo.searcher().search(query, filter_, collector);
    }

    public OpenBitSet collectKeys(Query filterQuery, String keyName, Query query) throws Exception {
        return collectKeys(filterQuery, keyName, query, true);
    }

    public OpenBitSet collectKeys(Query filterQuery, String keyName, Query query, boolean cacheCollectedKeys) throws Exception {
        return doCollectKeys(filterQuery, keyName, query);
    }

    private OpenBitSet doCollectKeys(Query filterQuery, String keyName, Query query) throws Exception {
        KeySuperCollector keyCollector = new KeySuperCollector(keyName);
        if (query == null)
            query = new MatchAllDocsQuery();
        search(query, filterQuery, keyCollector);
        return keyCollector.getCollectedKeys();
    }
    
    public Query createDrilldownQuery(Query luceneQuery, Map<String, String> drilldownQueries) {
        BooleanQuery q = new BooleanQuery(true);
        if (luceneQuery != null)
            q.add(luceneQuery, Occur.MUST);
        for (String field : drilldownQueries.keySet()) {
            String indexFieldName = facetsConfig.getDimConfig(field).indexFieldName;
            q.add(new TermQuery(DrillDownQuery.term(indexFieldName, field, drilldownQueries.get(field))), Occur.MUST); 
        }
        return q;
    }

    public QueryConverter getQueryConverter() {
        return new QueryConverter(this.facetsConfig);
    }
}

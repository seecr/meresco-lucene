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
import java.util.concurrent.ExecutionException;

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
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.spell.DirectSpellChecker;
import org.apache.lucene.search.spell.SuggestWord;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.OpenBitSet;
import org.apache.lucene.util.Version;
import org.meresco.lucene.LuceneResponse.ClusterHit;
import org.meresco.lucene.LuceneResponse.DedupHit;
import org.meresco.lucene.LuceneResponse.DrilldownData;
import org.meresco.lucene.LuceneResponse.Hit;
import org.meresco.lucene.LuceneResponse.GroupingHit;
import org.meresco.lucene.LuceneSettings.ClusterField;
import org.meresco.lucene.QueryConverter.FacetRequest;
import org.meresco.lucene.search.DeDupFilterSuperCollector;
import org.meresco.lucene.search.FacetSuperCollector;
import org.meresco.lucene.search.GroupSuperCollector;
import org.meresco.lucene.search.MerescoCluster;
import org.meresco.lucene.search.MerescoCluster.DocScore;
import org.meresco.lucene.search.MerescoCluster.TermScore;
import org.meresco.lucene.search.MerescoClusterer;
import org.meresco.lucene.search.MultiSuperCollector;
import org.meresco.lucene.search.SuperCollector;
import org.meresco.lucene.search.TopDocSuperCollector;
import org.meresco.lucene.search.TopFieldSuperCollector;
import org.meresco.lucene.search.TopScoreDocSuperCollector;
import org.meresco.lucene.search.join.AggregateScoreSuperCollector;
import org.meresco.lucene.search.join.KeySuperCollector;
import org.meresco.lucene.search.join.ScoreSuperCollector;

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
    private DirectSpellChecker spellChecker = new DirectSpellChecker();

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
        QueryData q = new QueryData();
        q.query = query;
        return executeQuery(q, null, null, null, null, null);
    }

    public LuceneResponse executeQuery(Query query, int start, int stop) throws Exception {
        QueryData q = new QueryData();
        q.query = query;
        q.start = start;
        q.stop = stop;
        return executeQuery(q, null, null, null, null, null);
    }

    public LuceneResponse executeQuery(Query query, List<FacetRequest> facets) throws Exception {
        QueryData q = new QueryData();
        q.query = query;
        q.facets = facets;
        return executeQuery(q, null, null, null, null, null);
    }

    public LuceneResponse executeQuery(QueryData q, List<Query> filterQueries, List<String[]> drilldownQueries, List<Filter> filters, List<AggregateScoreSuperCollector> scoreCollectors, Collection<KeySuperCollector> keyCollectors) throws IOException, InterruptedException, ExecutionException {
        int totalHits;
        List<LuceneResponse.Hit> hits;
        Collectors collectors = null;
        Map<String, Long> times = new HashMap<>();
        long t0 = System.currentTimeMillis();
        int topCollectorStop = q.stop;
        while (true) {
            collectors = createCollectors(q, topCollectorStop, keyCollectors, scoreCollectors);
            Filter f = filtersFor(filterQueries, filters == null ? null : filters.toArray(new Filter[0]));

            Query query = q.query;
            if (drilldownQueries != null) 
                query = createDrilldownQuery(query, drilldownQueries);
            long t1 = System.currentTimeMillis();
            indexAndTaxo.searcher().search(query, f, collectors.root);
            times.put("searchTime", System.currentTimeMillis() - t1);
            
            totalHits = collectors.topCollector.getTotalHits();
            if (q.clustering) {
                t1 = System.currentTimeMillis();
                hits = clusterTopDocsResponse(q, collectors, times);
                times.put("totalClusterTime", System.currentTimeMillis() - t1);
            } else {
                t1 = System.currentTimeMillis();
                hits = topDocsResponse(q, collectors);
                times.put("topDocsTime", System.currentTimeMillis() - t1);
            }
            
            if (hits.size() == q.stop - q.start || topCollectorStop >= totalHits)
                break;
            topCollectorStop *= 10;
        }
            
        LuceneResponse response = new LuceneResponse(totalHits);
        if (collectors.dedupCollector != null)
            response.totalWithDuplicates = collectors.dedupCollector.getTotalHits();
        
        response.hits = hits;
        
        if (collectors.facetCollector != null) {
            long t1 = System.currentTimeMillis();
            response.drilldownData = facetResult(collectors.facetCollector, q.facets);
            times.put("facetTime", System.currentTimeMillis() - t1);
        }

        if (q.suggestionRequest != null) {
            long t1 = System.currentTimeMillis();
            HashMap<String, SuggestWord[]> result = new HashMap<>();  
            for (String suggest : q.suggestionRequest.suggests)
                result.put(suggest, suggest(suggest, q.suggestionRequest.count, q.suggestionRequest.field));
            times.put("suggestionTime", System.currentTimeMillis() - t1);
            response.suggestions = result;
        }
        response.times = times;
        response.queryTime = System.currentTimeMillis() - t0;
        return response;
    }

    private List<Hit> clusterTopDocsResponse(QueryData q, Collectors collectors, Map<String, Long> times) throws IOException {
        int totalHits = collectors.topCollector.getTotalHits();
                
        List<LuceneResponse.Hit> hits = new ArrayList<>();
        double epsilon = interpolateEpsilon(totalHits, q.stop - q.start);
        MerescoClusterer clusterer = new MerescoClusterer(indexAndTaxo.reader, epsilon, settings.clusteringMinPoints);
        for (ClusterField clusterField : settings.clusterFields)
            clusterer.registerField(clusterField.fieldname, clusterField.weight, clusterField.filterValue);
        TopDocs topDocs = collectors.topCollector.topDocs(q.start);
        long t0 = System.currentTimeMillis();
        clusterer.processTopDocs(topDocs);
        times.put("processTopDocsForClustering", System.currentTimeMillis() - t0);
        t0 = System.currentTimeMillis();
        clusterer.finish();
        times.put("clusteringAlgorithm", System.currentTimeMillis() - t0);
        int count = q.start;
        HashSet<Integer> seenDocIds = new HashSet<>();
        t0 = System.currentTimeMillis();
        for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
            if (count >= q.stop)
                break;
            if (seenDocIds.contains(scoreDoc.doc))
                continue;
            MerescoCluster cluster = clusterer.cluster(scoreDoc.doc);
            DocScore[] clusterTopDocs = cluster == null ? new DocScore[0] : cluster.topDocs;
            
            List<Integer> clusteredDocIds = new ArrayList<>();
            if (cluster == null)
                clusteredDocIds.add(scoreDoc.doc);
            else {
                for (DocScore ds : clusterTopDocs) {
                    clusteredDocIds.add(ds.docId);
                }
            }
            seenDocIds.addAll(clusteredDocIds);
            ClusterHit hit = new ClusterHit(getDocument(clusteredDocIds.get(0)).get(ID_FIELD), scoreDoc.score);
            hit.topDocs = clusterTopDocs;
            for (DocScore docScore : hit.topDocs)
                docScore.identifier = getDocument(docScore.docId).get(ID_FIELD);
            hit.topTerms = cluster == null ? new TermScore[0] : cluster.topTerms;
            hits.add(hit);
            count += 1;
        }
        times.put("collectClusters", System.currentTimeMillis() - t0);
        return hits;
    }

    private List<Hit> topDocsResponse(QueryData q, Collectors collectors) throws IOException {
        int totalHits = collectors.topCollector.getTotalHits();
                
        DeDupFilterSuperCollector dedupCollector = collectors.dedupCollector;
        GroupSuperCollector groupingCollector = collectors.groupingCollector;
        
        HashSet<String> seenIds = new HashSet<>();
        int count = q.start;
        List<LuceneResponse.Hit> hits = new ArrayList<>();
        for (ScoreDoc scoreDoc : collectors.topCollector.topDocs(q.stop == 0 ? 1 : q.start).scoreDocs) { //TODO: temp fix for start/stop = 0
            if (count >= q.stop)
                break;
            if (dedupCollector != null) {
                DeDupFilterSuperCollector.Key keyForDocId = dedupCollector.keyForDocId(scoreDoc.doc);
                int newDocId = keyForDocId == null ? scoreDoc.doc : keyForDocId.getDocId();
                DedupHit hit = new DedupHit(getDocument(newDocId).get(ID_FIELD), scoreDoc.score);
                hit.duplicateField = dedupCollector.getKeyName();
                hit.duplicateCount = 1;
                if (keyForDocId != null)
                    hit.duplicateCount = keyForDocId.getCount();
                hit.score = scoreDoc.score;
                hits.add(hit);
            } else if (groupingCollector != null) { 
                GroupingHit hit = new GroupingHit(getDocument(scoreDoc.doc).get(ID_FIELD), scoreDoc.score);
                if (seenIds.contains(hit.id))
                    continue;
                
                List<String> duplicateIds = new ArrayList<>();
                duplicateIds.add(hit.id);
                if (totalHits > (q.stop - q.start)) {
                    List<Integer> groupedDocIds = groupingCollector.group(scoreDoc.doc);
                    if (groupedDocIds != null)
                        for (int docId : groupedDocIds) {
                            String id = getDocument(docId).get(ID_FIELD);
                            if (!id.equals(hit.id))
                                duplicateIds.add(id);
                        }
                }
                seenIds.addAll(duplicateIds);
                hit.groupingField = groupingCollector.getKeyName();
                hit.duplicates = duplicateIds;
                hit.score = scoreDoc.score;
                hits.add(hit);
            } else {
                Hit hit = new Hit(getDocument(scoreDoc.doc).get(ID_FIELD), scoreDoc.score);
                hits.add(hit);
            }
            count++;
        }
        return hits;
    }

    public List<DrilldownData> facets(List<FacetRequest> facets, List<Query> filterQueries, List<String[]> drilldownQueries, Filter filter) throws Exception {
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

    private Collectors createCollectors(QueryData q, int stop, Collection<KeySuperCollector> keyCollectors, List<AggregateScoreSuperCollector> scoreCollectors) {
        Collectors allCollectors = new Collectors();
        SuperCollector<?> resultsCollector;
        if (q.clustering) {
            allCollectors.topCollector = topCollector(q.start, stop + settings.clusterMoreRecords, q.sort);
            resultsCollector = allCollectors.topCollector;
        } else if (q.groupingField != null) {
            allCollectors.topCollector = topCollector(q.start, stop * 10, q.sort);
            allCollectors.groupingCollector = new GroupSuperCollector(q.groupingField, allCollectors.topCollector);
            resultsCollector = allCollectors.groupingCollector;
        } else if (q.dedupField != null) {
            allCollectors.topCollector = topCollector(q.start, stop, q.sort);
            allCollectors.dedupCollector = new DeDupFilterSuperCollector(q.dedupField, q.dedupSortField, allCollectors.topCollector);
            resultsCollector = allCollectors.dedupCollector;
        } else {
            allCollectors.topCollector = topCollector(q.start, stop, q.sort);
            resultsCollector = allCollectors.topCollector;
        }
        allCollectors.facetCollector = facetCollector(q.facets);

        List<SuperCollector<?>> collectors = new ArrayList<SuperCollector<?>>();
        collectors.add(resultsCollector);
        if (allCollectors.facetCollector != null) {
            collectors.add(allCollectors.facetCollector);
        }
        if (keyCollectors != null)
            collectors.addAll(keyCollectors);
        allCollectors.root = new MultiSuperCollector(collectors);
        
        if (scoreCollectors != null && scoreCollectors.size() > 0) {
            for (AggregateScoreSuperCollector scoreCollector : scoreCollectors) {
                scoreCollector.setDelegate(allCollectors.root);
                allCollectors.root = scoreCollector;
            }
        }
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
            dd.path = facet.path;
            List<DrilldownData.Term> terms = drilldownDataFromFacetResult(facetCollector, facet, facet.path, this.facetsConfig.getDimConfig(facet.fieldname).hierarchical);
            if (terms != null) {
                dd.terms = terms;
                drilldownData.add(dd);
            }
        }
        return drilldownData;
    }
    
    public List<DrilldownData.Term> drilldownDataFromFacetResult(FacetSuperCollector facetCollector, FacetRequest facet, String[] path, boolean hierarchical) throws IOException {
        FacetResult result = facetCollector.getTopChildren(facet.maxTerms == 0 ? Integer.MAX_VALUE : facet.maxTerms, facet.fieldname, path);
        if (result == null)
            return null;
        List<DrilldownData.Term> terms = new ArrayList<DrilldownData.Term>();
        for (LabelAndValue l : result.labelValues) {
            DrilldownData.Term term = new DrilldownData.Term(l.label, l.value.intValue());
            if (hierarchical) {
                String[] newPath = new String[path.length + 1];
                System.arraycopy(path, 0, newPath, 0, path.length);
                newPath[newPath.length - 1] = l.label;
                term.subTerms = drilldownDataFromFacetResult(facetCollector, facet, newPath, hierarchical);
            }
            terms.add(term);
        }
        return terms;
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
        public GroupSuperCollector groupingCollector;
        public DeDupFilterSuperCollector dedupCollector;
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
    
    public Query createDrilldownQuery(Query luceneQuery, List<String[]> drilldownQueries) {
        BooleanQuery q = new BooleanQuery(true);
        if (luceneQuery != null)
            q.add(luceneQuery, Occur.MUST);
        for (int i = 0; i<drilldownQueries.size(); i+=2) {
            String field = drilldownQueries.get(i)[0];
            String indexFieldName = facetsConfig.getDimConfig(field).indexFieldName;
            q.add(new TermQuery(DrillDownQuery.term(indexFieldName, field, drilldownQueries.get(i+1))), Occur.MUST); 
        }
        return q;
    }

    public QueryConverter getQueryConverter() { // TODO: Test?
        return new QueryConverter(this.facetsConfig);
    }
    
    public ScoreSuperCollector scoreCollector(String keyName, Query query) throws Exception {
// TODO:        return self._scoreCollectorCache.get((keyName, query))
        return doScoreCollecting(keyName, query);
    }
    
    public ScoreSuperCollector doScoreCollecting(String keyName, Query query) throws Exception {
        ScoreSuperCollector scoreCollector = new ScoreSuperCollector(keyName);
        indexAndTaxo.searcher().search(query, null, scoreCollector);
        return scoreCollector;
    }

    public SuggestWord[] suggest(String term, int count, String field) throws IOException {
        return spellChecker.suggestSimilar(new Term(field, term), count, indexAndTaxo.searcher().getIndexReader());
    }

    double interpolateEpsilon(int hits, int slice) {
        double eps = settings.clusteringEPS * (hits - slice) / settings.clusterMoreRecords;
        return Math.max(Math.min(eps, settings.clusteringEPS), 0.0);
    }
}

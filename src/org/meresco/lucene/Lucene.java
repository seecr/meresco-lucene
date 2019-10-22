/* begin license *
 *
 * "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
 *
 * Copyright (C) 2015-2016, 2019 Koninklijke Bibliotheek (KB) http://www.kb.nl
 * Copyright (C) 2015-2016, 2019 Seecr (Seek You Too B.V.) https://seecr.nl
 * Copyright (C) 2016 Stichting Kennisnet http://www.kennisnet.nl
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

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.commons.collections4.map.LRUMap;
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
import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager;
import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager.SearcherAndTaxonomy;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.taxonomy.TaxonomyReader.ChildrenIterator;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.facet.taxonomy.writercache.LruTaxonomyWriterCache;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MultiTerms;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.queries.CommonTermsQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ReferenceManager.RefreshListener;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.spell.DirectSpellChecker;
import org.apache.lucene.search.spell.SuggestMode;
import org.apache.lucene.search.spell.SuggestWord;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.meresco.lucene.JsonQueryConverter.FacetRequest;
import org.meresco.lucene.LuceneResponse.ClusterHit;
import org.meresco.lucene.LuceneResponse.DedupHit;
import org.meresco.lucene.LuceneResponse.DrilldownData;
import org.meresco.lucene.LuceneResponse.Hit;
import org.meresco.lucene.search.DeDupFilterSuperCollector;
import org.meresco.lucene.search.FacetSuperCollector;
import org.meresco.lucene.search.MerescoCluster;
import org.meresco.lucene.search.MerescoCluster.DocScore;
import org.meresco.lucene.search.MerescoClusterer;
import org.meresco.lucene.search.MultiSuperCollector;
import org.meresco.lucene.search.SuperCollector;
import org.meresco.lucene.search.SuperIndexSearcher;
import org.meresco.lucene.search.TopDocSuperCollector;
import org.meresco.lucene.search.TopFieldSuperCollector;
import org.meresco.lucene.search.TopScoreDocSuperCollector;
import org.meresco.lucene.search.join.AggregateScoreSuperCollector;
import org.meresco.lucene.search.join.KeySuperCollector;
import org.meresco.lucene.search.join.ScoreSuperCollector;


public class Lucene {
    @SuppressWarnings("serial")
    public static class UninitializedException extends Exception {
    }

    public static final String ID_FIELD = "__id__";
    private int commitCount = 0;
    private Timer commitTimer;
    public String name;
    private Path stateDir;
    private Map<String, OrdinalsReader> ordinalsReaders = new HashMap<>();
    private DirectSpellChecker spellChecker = new DirectSpellChecker();
    LuceneData data = new LuceneData();
	private JsonQueryConverter queryConverter;


    public Lucene(String name, Path stateDir) {
        this.name = name;
        this.stateDir = stateDir;
    }

    public Lucene(Path stateDir, LuceneSettings settings) throws Exception {
        this(null, stateDir, settings);
    }

    public Lucene(String name, Path stateDir, LuceneSettings settings) throws Exception {
        this.name = name;
        this.stateDir = stateDir;
        initSettings(settings);
    }

    public void initSettings(LuceneSettings settings) throws Exception {
        data.initSettings(stateDir, settings);
    }

    public LuceneSettings getSettings() throws Exception {
        return this.data.getSettings();
    }

    public boolean hasSettings() {
        return this.data.hasSettings();
    }

    public synchronized void close() throws IOException {
        if (commitTimer != null)
            commitTimer.cancel();
        this.data.close();
    }

    public void addDocument(Document doc) throws Exception {
        doc = data.getFacetsConfig().build(data.getTaxoWriter(), doc);
        data.getIndexWriter().addDocument(doc);
        maybeCommitAfterUpdate();
    }

    public void addDocument(String identifier, Document doc) throws Exception {
        doc.add(new StringField(ID_FIELD, identifier, Store.YES));
        doc = data.getFacetsConfig().build(data.getTaxoWriter(), doc);
        data.getIndexWriter().updateDocument(new Term(ID_FIELD, identifier), doc);
        maybeCommitAfterUpdate();
    }

    public void deleteDocument(String identifier) throws Exception {
        data.getIndexWriter().deleteDocuments(new Term(ID_FIELD, identifier));
        maybeCommitAfterUpdate();
    }

    public void deleteDocuments(Query query) throws Exception {
        data.getIndexWriter().deleteDocuments(query);
        maybeCommitAfterUpdate();
    }

    public void maybeCommitAfterUpdate() throws Exception {
        commitCount++;
        LuceneSettings settings = data.getSettings();
        if (commitCount >= settings.commitCount) {
            commit();
            return;
        }
        if (commitTimer == null) {
            TimerTask timerTask = new TimerTask() {
                @Override
                public void run() {
                    try {
                        commit();
                    } catch (Exception e) {
                        throw new RuntimeException();
                    }
                }
            };
            commitTimer = new Timer();
            commitTimer.schedule(timerTask, settings.commitTimeout * 1000);
        }
    }

    public synchronized void commit() throws Exception {
        commitCount = 0;
        if (commitTimer != null) {
            commitTimer.cancel();
            commitTimer.purge();
            commitTimer = null;
        }
        data.commit();
    }

    public LuceneResponse executeQuery(QueryData q) throws Throwable {
        return executeQuery(q, null, null, null, null, null);
    }

    public LuceneResponse executeQuery(Query query) throws Throwable {
        QueryData q = new QueryData();
        q.query = query;
        return executeQuery(q, null, null, null, null, null);
    }

    public LuceneResponse executeQuery(Query query, int start, int stop) throws Throwable {
        QueryData q = new QueryData();
        q.query = query;
        q.start = start;
        q.stop = stop;
        return executeQuery(q, null, null, null, null, null);
    }

    public LuceneResponse executeQuery(Query query, List<FacetRequest> facets) throws Throwable {
        QueryData q = new QueryData();
        q.query = query;
        q.facets = facets;
        return executeQuery(q, null, null, null, null, null);
    }

    public LuceneResponse executeQuery(QueryData q, List<Query> filterQueries, List<String[]> drilldownQueries, List<Query> filters, List<AggregateScoreSuperCollector> scoreCollectors,
            Collection<KeySuperCollector> keyCollectors) throws Throwable {
        List<LuceneResponse.Hit> hits;
        Collectors collectors;
        Map<String, Long> times = new HashMap<>();
        long t0 = System.currentTimeMillis();
        int topCollectorStop = q.stop;
        int moreRecords = 0;
        SearcherAndTaxonomy reference = data.getManager().acquire();

        ClusterConfig clusterConfig = null;
        if (q.clustering) {
            clusterConfig = q.clusterConfig;
            if (clusterConfig == null) {
                clusterConfig = data.getSettings().clusterConfig;
            }
            if (clusterConfig != null) {
                moreRecords = clusterConfig.clusterMoreRecords;
            }
        }
        if (q.dedupField != null) {
            moreRecords = 100;
        }

        Query filter = filtersFor(filterQueries, filters == null ? null : filters.toArray(new Query[0]));

        Query query = mergeQueryAndFilter(q.query, filter);
        if (drilldownQueries != null) {
            query = createDrilldownQuery(query, drilldownQueries);
        }

        try {
            long totalHits=0;
            long adjustedTotalHits=0;

            boolean isFirstLoop = true;

            while (true) {
                collectors = createCollectors(q, topCollectorStop + moreRecords, keyCollectors, scoreCollectors, reference);

                long t1 = System.currentTimeMillis();
                ((SuperIndexSearcher) reference.searcher).search(query, collectors.root);
                times.put("searchTime", System.currentTimeMillis() - t1);

                if (isFirstLoop) {
                    totalHits = collectors.topCollector.getTotalHits();
                    if (collectors.dedupCollector != null) {
                        adjustedTotalHits = collectors.dedupCollector.adjustTotalHits(totalHits);
                    }
                    else {
                        adjustedTotalHits = totalHits;
                    }
                }
                isFirstLoop = false;

                if (q.clustering) {
                    t1 = System.currentTimeMillis();
                    hits = clusterTopDocsResponse(q, collectors, times, reference.searcher.getIndexReader(), clusterConfig);
                    times.put("totalClusterTime", System.currentTimeMillis() - t1);
                } else {
                    t1 = System.currentTimeMillis();
                    hits = topDocsResponse(q, collectors);
                    times.put("topDocsTime", System.currentTimeMillis() - t1);
                }

                if (hits.size() == q.stop - q.start || (topCollectorStop + moreRecords) >= totalHits) {
                    break;
                }
                topCollectorStop *= 10;
                if (topCollectorStop > 10000) {
                    break;
                }

            }

            LuceneResponse response = new LuceneResponse(adjustedTotalHits);
            if (collectors.dedupCollector != null)
                response.totalWithDuplicates = collectors.topCollector.getTotalHits();

            response.hits = hits;

            if (collectors.facetCollector != null) {
                long t1 = System.currentTimeMillis();
                response.drilldownData = facetResult(collectors.facetCollector, q.facets);
                times.put("facetTime", System.currentTimeMillis() - t1);
            }

            if (q.suggestionRequest != null) {
                long t1 = System.currentTimeMillis();
                HashMap<String, SuggestWord[]> result = new HashMap<>();
                for (String suggest : q.suggestionRequest.suggests) {
                    result.put(suggest, suggest(suggest, q.suggestionRequest.count, q.suggestionRequest.field, q.suggestionRequest.mode));
                }
                times.put("suggestionTime", System.currentTimeMillis() - t1);
                response.suggestions = result;
            }
            response.times = times;
            response.queryTime = System.currentTimeMillis() - t0;
            return response;
        } finally {
            data.getManager().release(reference);
        }
    }

    private Query mergeQueryAndFilter(Query query, Query filter) {
        if (filter == null) {
            return query;
        }
        return new BooleanQuery.Builder()
                .add(query, BooleanClause.Occur.MUST)
                .add(filter, BooleanClause.Occur.FILTER)
                .build();
    }

    private List<Hit> clusterTopDocsResponse(QueryData q, Collectors collectors, Map<String, Long> times, IndexReader indexReader, ClusterConfig clusterConfig) throws Exception {
        long totalHits = collectors.topCollector.getTotalHits();
        TopDocs topDocs = collectors.topCollector.topDocs(q.start);

        MerescoClusterer clusterer = new MerescoClusterer(indexReader, clusterConfig, this.getSettings().interpolateEpsilon, totalHits, q.stop - q.start);
        long t0 = System.currentTimeMillis();
        clusterer.processTopDocs(topDocs);
        times.put("processTopDocsForClustering", System.currentTimeMillis() - t0);
        t0 = System.currentTimeMillis();
        clusterer.finish();
        times.put("clusteringAlgorithm", System.currentTimeMillis() - t0);

        List<LuceneResponse.Hit> hits = new ArrayList<>();
        int count = q.start;
        HashSet<Integer> seenDocIds = new HashSet<>();
        t0 = System.currentTimeMillis();
        for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
            if (count >= q.stop)
                break;
            if (seenDocIds.contains(scoreDoc.doc))
                continue;

            Integer representative = null;
            MerescoCluster cluster = clusterer.cluster(scoreDoc.doc);
            if (cluster == null) {
                representative = scoreDoc.doc;
                seenDocIds.add(representative);
            } else {
                for (DocScore ds : cluster.topDocs) {
                    if (representative == null) {
                        representative = ds.docId;
                    }
                    seenDocIds.add(ds.docId);
                }
            }

            ClusterHit hit = new ClusterHit(getDocument(representative).get(ID_FIELD), scoreDoc.score);
            if (cluster != null) {
                hit.topTerms = cluster.topTerms;
                hit.topDocs = cluster.topDocs;
                for (DocScore docScore : cluster.topDocs) {
                    docScore.identifier = getDocument(docScore.docId).get(ID_FIELD);
                }
            }
            hits.add(hit);
            count += 1;
        }
        times.put("collectClusters", System.currentTimeMillis() - t0);
        return hits;
    }

    private List<Hit> topDocsResponse(QueryData q, Collectors collectors) throws Exception {
        List<LuceneResponse.Hit> hits = new ArrayList<>();
        DeDupFilterSuperCollector dedupCollector = collectors.dedupCollector;

        int startAt = q.stop == 0 ? 1 : q.start; // TODO: temp fix for start/stop = 0
        if (dedupCollector == null) {
            int count = startAt;
            for (ScoreDoc scoreDoc : collectors.topCollector.topDocs(startAt).scoreDocs) {
                if (count >= q.stop) {
                    break;
                }
                Document document = getDocument(scoreDoc.doc);
                Hit hit = new Hit(document.get(ID_FIELD), scoreDoc.score);
                for (String storedField : q.storedFields) {
                    hit.fields.add(document.getFields(storedField));
                }
                hits.add(hit);
                count++;
            }
        }
        else {
            HashSet<Long> seenIds = new HashSet<>();
            int count = 0;
            TopDocs topDocs = collectors.topCollector.topDocs(0);
            for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                if (count >= q.stop) {
                    break;
                }
                DeDupFilterSuperCollector.Key key = dedupCollector.keyForDocId(scoreDoc.doc);
                Long deDupKey = key != null ? key.getDeDupKey() : null;
                if (deDupKey != null) {
                    if (seenIds.contains(deDupKey)) {
                        continue;
                    }
                    seenIds.add(deDupKey);
                }
                if (count >= startAt) {
                    DeDupFilterSuperCollector.Key dedupKey;
                    int newDocId;
                    if (deDupKey==null) {
                        dedupKey = null;
                        newDocId = scoreDoc.doc;
                    }
                    else {
                        dedupKey = dedupCollector.keyForDocId(scoreDoc.doc);
                        newDocId = dedupKey.getDocId();
                    }

                    Document document = getDocument(newDocId);
                    DedupHit dedupHit = new DedupHit(document.get(ID_FIELD), scoreDoc.score);
                    dedupHit.duplicateField = dedupCollector.getKeyName();
                    dedupHit.duplicateCount = 1;
                    if (dedupKey != null) {
                        dedupHit.duplicateCount = dedupKey.getCount();
                    }
                    dedupHit.score = scoreDoc.score;
                    Hit hit = dedupHit;
                    for (String storedField : q.storedFields) {
                        hit.fields.add(document.getFields(storedField));
                    }
                    hits.add(hit);
                }
                count++;
            }
        }
        return hits;
    }

    public List<DrilldownData> facets(List<FacetRequest> facets, List<Query> filterQueries, List<String[]> drilldownQueries, Query filter) throws Throwable {
        SearcherAndTaxonomy reference = data.getManager().acquire();
        try {
            FacetSuperCollector facetCollector = facetCollector(facets, reference.taxonomyReader);
            if (facetCollector == null)
                return new ArrayList<>();
            Query filter_ = filtersFor(filterQueries, filter);
            Query query = new MatchAllDocsQuery();
            if (drilldownQueries != null)
                query = createDrilldownQuery(query, drilldownQueries);
            ((SuperIndexSearcher) reference.searcher).search(mergeQueryAndFilter(query, filter_), facetCollector);
            return facetResult(facetCollector, facets);
        } finally {
            data.getManager().release(reference);
        }
    }

    private Query filtersFor(List<Query> filterQueries, Query... filter) throws Exception {
        List<Query> filters = new ArrayList<>();
        if (filterQueries != null)
            for (Query query : filterQueries)
                filters.add(query);
        if (filter != null)
            for (Query f : filter)
                if (f != null)
                    filters.add(f);
        if (filters.size() == 0)
            return null;
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        filters.forEach(f -> builder.add(f, Occur.FILTER));
        return builder.build();
    }

    public Document getDocument(int docID) throws Exception {
        SearcherAndTaxonomy reference = data.getManager().acquire();
        try {
            return ((SuperIndexSearcher) reference.searcher).doc(docID);
        } finally {
            data.getManager().release(reference);
        }
    }

    private Collectors createCollectors(QueryData q, int stop, Collection<KeySuperCollector> keyCollectors, List<AggregateScoreSuperCollector> scoreCollectors, SearcherAndTaxonomy reference)
            throws Exception {
        Collectors allCollectors = new Collectors();
        allCollectors.topCollector = topCollector(q.start, stop, q.sort);
        SuperCollector<?> resultsCollector = allCollectors.topCollector;
        if (q.dedupField != null) {
            allCollectors.dedupCollector = new DeDupFilterSuperCollector(q.dedupField, q.dedupSortFields, allCollectors.topCollector);
            resultsCollector = allCollectors.dedupCollector;
        }
        allCollectors.facetCollector = facetCollector(q.facets, reference.taxonomyReader);

        List<SuperCollector<?>> collectors = new ArrayList<>();
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
            // TODO: temp fix for start/stop = 0; You should use TotalHitCountSuperCollector
            return new TopScoreDocSuperCollector(stop == 0 ? 1 : stop);
        // return new TotalHitCountSuperCollector();
        if (sort == null)
            return new TopScoreDocSuperCollector(stop);
        return new TopFieldSuperCollector(sort, stop, true, false);
    }

    private FacetSuperCollector facetCollector(List<FacetRequest> facets, TaxonomyReader taxonomyReader) throws Exception {
        if (facets == null || facets.size() == 0)
            return null;
        String[] indexFieldnames = getIndexFieldNames(facets);
        FacetSuperCollector collector = new FacetSuperCollector(taxonomyReader, data.getFacetsConfig(), getOrdinalsReader(indexFieldnames[0]));
        for (int i = 1; i < indexFieldnames.length; i++) {
            collector.addOrdinalsReader(getOrdinalsReader(indexFieldnames[i]));
        }
        return collector;
    }

    String[] getIndexFieldNames(List<FacetRequest> facets) throws Exception {
        Set<String> indexFieldnames = new HashSet<>();
        for (FacetRequest f : facets)
            indexFieldnames.add(this.data.getFacetsConfig().getDimConfig(f.fieldname).indexFieldName);
        return indexFieldnames.toArray(new String[0]);
    }

    private OrdinalsReader getOrdinalsReader(String indexFieldname) throws Exception {
        OrdinalsReader reader = ordinalsReaders.get(indexFieldname);
        if (reader == null) {
            reader = indexFieldname == null ? new DocValuesOrdinalsReader() : new DocValuesOrdinalsReader(indexFieldname);
            if (this.getSettings().cacheFacetOrdinals)
                reader = new CachedOrdinalsReader(reader);
            ordinalsReaders.put(indexFieldname, reader);
        }
        return reader;
    }

    private List<DrilldownData> facetResult(FacetSuperCollector facetCollector, List<FacetRequest> facets) throws Exception {
        List<DrilldownData> drilldownData = new ArrayList<>();
        for (FacetRequest facet : facets) {
            DrilldownData dd = new DrilldownData(facet.fieldname);
            dd.path = facet.path;
            List<DrilldownData.Term> terms = drilldownDataFromFacetResult(facetCollector, facet, facet.path, this.data.getFacetsConfig().getDimConfig(facet.fieldname).hierarchical);
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
        List<DrilldownData.Term> terms = new ArrayList<>();
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

        // if t == str:
        // convert = lambda term: term.utf8ToString()
        // elif t == int:
        // convert = lambda term: NumericUtils.prefixCodedToInt(term)
        // elif t == long:
        // convert = lambda term: NumericUtils.prefixCodedToLong(term)
        // elif t == float:
        // convert = lambda term: NumericUtils.sortableLongToDouble(NumericUtils.prefixCodedToLong(term))

        SearcherAndTaxonomy reference = data.getManager().acquire();
        try {
            List<TermCount> terms = new ArrayList<>();
            IndexReader reader = reference.searcher.getIndexReader();
            Terms termsEnum = MultiTerms.getTerms(reader, field);
            if (termsEnum == null)
                return terms;
            TermsEnum iterator = termsEnum.iterator();
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
        } finally {
            data.getManager().release(reference);
        }
    }

    public int numDocs() throws Exception {
        return this.data.getIndexWriter().getDocStats().numDocs;
    }

    public int maxDoc() throws Exception {
        return this.data.getIndexWriter().getDocStats().maxDoc;
    }

    public List<String> fieldnames() throws Exception {
        SearcherAndTaxonomy reference = data.getManager().acquire();
        try {
            List<String> fieldnames = new ArrayList<>();
            FieldInfos fields = FieldInfos.getMergedFieldInfos(reference.searcher.getIndexReader());
            if (fields == null)
                return fieldnames;
            for (Iterator<FieldInfo> iterator = fields.iterator(); iterator.hasNext();) {
                fieldnames.add(iterator.next().name);
            }
            Collections.sort(fieldnames);
            return fieldnames;
        } finally {
            data.getManager().release(reference);
        }
    }

    public List<String> drilldownFieldnames(int limit, String dim, String... path) throws Exception {
        SearcherAndTaxonomy reference = data.getManager().acquire();
        try {
            DirectoryTaxonomyReader taxoReader = reference.taxonomyReader;
            int parentOrdinal = dim == null ? TaxonomyReader.ROOT_ORDINAL : taxoReader.getOrdinal(dim, path);
            ChildrenIterator childrenIter = taxoReader.getChildren(parentOrdinal);
            List<String> fieldnames = new ArrayList<>();
            while (true) {
                int ordinal = childrenIter.next();
                if (ordinal == TaxonomyReader.INVALID_ORDINAL)
                    break;
                String[] components = taxoReader.getPath(ordinal).components;
                fieldnames.add(components[components.length - 1]);
                if (fieldnames.size() >= limit)
                    break;
            }
            return fieldnames;
        } finally {
            data.getManager().release(reference);
        }
    }

    public void search(Query query, Query filterQuery, Collector collector) throws Throwable {
        SearcherAndTaxonomy reference = data.getManager().acquire();
        try {
            ((SuperIndexSearcher) reference.searcher).search(mergeQueryAndFilter(query, filterQuery), collector);
        } finally {
            data.getManager().release(reference);
        }
    }

    public void search(Query query, Query filterQuery, SuperCollector<?> collector) throws Throwable {
        SearcherAndTaxonomy reference = data.getManager().acquire();
        try {
            ((SuperIndexSearcher) reference.searcher).search(mergeQueryAndFilter(query, filterQuery), collector);
        } finally {
            data.getManager().release(reference);
        }
    }

    public void search(Query q, KeySuperCollector keyCollector) throws Throwable {
        this.search(q, null, keyCollector);
    }

    public FixedBitSet collectKeys(Query filterQuery, String keyName, Query query) throws Throwable {
        return collectKeys(filterQuery, keyName, query, true);
    }

    public FixedBitSet collectKeys(Query filterQuery, String keyName, Query query, boolean cacheCollectedKeys) throws Throwable {
        if (cacheCollectedKeys) {
            KeyNameQuery keyNameQuery = new KeyNameQuery(keyName, filterQuery);
            FixedBitSet keys = data.getKeyCollectorCache().get(keyNameQuery);
            if (keys == null) {
                keys = doCollectKeys(filterQuery, keyName, query);
                data.getKeyCollectorCache().put(keyNameQuery, keys);
            }
            return keys;
        }
        return doCollectKeys(filterQuery, keyName, query);
    }

    private FixedBitSet doCollectKeys(Query filterQuery, String keyName, Query query) throws Throwable {
        KeySuperCollector keyCollector = new KeySuperCollector(keyName);
        if (query == null) {
            query = new MatchAllDocsQuery();
        }
        search(query, filterQuery, keyCollector);
        return keyCollector.getCollectedKeys();
    }

    public Query createDrilldownQuery(Query luceneQuery, List<String[]> drilldownQueries) throws Exception {
        BooleanQuery.Builder q = new BooleanQuery.Builder(); // TODO: disableCoord ??
        if (luceneQuery != null) {
            q.add(luceneQuery, Occur.MUST);
        }
        for (int i = 0; i < drilldownQueries.size(); i += 2) {
            String field = drilldownQueries.get(i)[0];
            String indexFieldName = data.getFacetsConfig().getDimConfig(field).indexFieldName;
            q.add(new TermQuery(DrillDownQuery.term(indexFieldName, field, drilldownQueries.get(i + 1))), Occur.MUST);
        }
        return q.build();
    }

    public JsonQueryConverter getQueryConverter() throws Exception {
    	if (this.queryConverter == null) {
    		this.queryConverter = new JsonQueryConverter(this.data.getFacetsConfig(), this.name);
    	}
    	return this.queryConverter;
    }

    public ScoreSuperCollector scoreCollector(String keyName, Query query) throws Throwable {
        KeyNameQuery keyNameQuery = new KeyNameQuery(keyName, query);
        ScoreSuperCollector scoreCollector = data.getScoreCollectorCache().get(keyNameQuery);
        if (scoreCollector == null) {
            scoreCollector = doScoreCollecting(keyName, query);
            data.getScoreCollectorCache().put(keyNameQuery, scoreCollector);
        }
        return scoreCollector;
    }

    public ScoreSuperCollector doScoreCollecting(String keyName, Query query) throws Throwable {
        SearcherAndTaxonomy reference = data.getManager().acquire();
        try {
            ScoreSuperCollector scoreCollector = new ScoreSuperCollector(keyName);
            ((SuperIndexSearcher) reference.searcher).search(query, scoreCollector);
            return scoreCollector;
        } finally {
            data.getManager().release(reference);
        }
    }

    public SuggestWord[] suggest(String term, int count, String field) throws Exception {
        return suggest(term, count, field, SuggestMode.SUGGEST_WHEN_NOT_IN_INDEX);
    }


    public SuggestWord[] suggest(String term, int count, String field, SuggestMode suggestMode) throws Exception {
        SearcherAndTaxonomy reference = data.getManager().acquire();
        try {
            return spellChecker.suggestSimilar(new Term(field, term), count, reference.searcher.getIndexReader(), suggestMode);
        } finally {
            data.getManager().release(reference);
        }
    }

    public LuceneResponse similarDocuments(String identifier) throws Throwable {
        SearcherAndTaxonomy reference = data.getManager().acquire();
        try {
            Query idQuery = new TermQuery(new Term(ID_FIELD, identifier));
            TopDocs topDocs = reference.searcher.search(idQuery, 1);
            if (topDocs.totalHits.value == 0)
                return new LuceneResponse(0);
            int docId = topDocs.scoreDocs[0].doc;
            IndexReader reader = reference.searcher.getIndexReader();
            CommonTermsQuery commonQuery = new CommonTermsQuery(Occur.SHOULD, Occur.SHOULD, 0.1f);
            Fields termVectors = reader.getTermVectors(docId);
            if (termVectors == null)
                return new LuceneResponse(0);
            for (String field : termVectors) {
                TermsEnum iterator = termVectors.terms(field).iterator();
                BytesRef b;
                while ((b = iterator.next()) != null) {
                    Term term = new Term(field, b.utf8ToString());
                    commonQuery.add(term);
                }
            }
            BooleanQuery.Builder query = new BooleanQuery.Builder();
            query.add(idQuery, Occur.MUST_NOT);
            query.add(commonQuery, Occur.MUST);
            return executeQuery(query.build());
        } finally {
            data.getManager().release(reference);
        }
    }

    public static class Collectors {
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

    private static class KeyNameQuery {
        private String keyName;
        private Query query;

        public KeyNameQuery(String keyName, Query query) {
            this.keyName = keyName;
            this.query = query;
        }

        @Override
        public int hashCode() {
            return keyName.hashCode() + 127 * query.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof KeyNameQuery) {
                KeyNameQuery other = (KeyNameQuery) obj;
                return other.keyName.equals(keyName) && other.query.equals(query);
            }
            return false;
        }
    }


    static class LuceneData {
        private IndexWriter indexWriter;
        private DirectoryTaxonomyWriter taxoWriter;
        private LuceneSettings settings;
        private Map<KeyNameQuery, ScoreSuperCollector> scoreCollectorCache;
        private Map<KeyNameQuery, FixedBitSet> keyCollectorCache;
        private SearcherTaxonomyManager manager;
        private LuceneRefreshListener refreshListener = new LuceneRefreshListener();

        public void commit() throws Exception {
            this.indexWriter.commit();
            this.taxoWriter.commit();
            this.manager.maybeRefreshBlocking();
            if (this.refreshListener.isRefreshed()) {
                this.scoreCollectorCache.clear();
                this.keyCollectorCache.clear();
            }
        }

		public void close() throws IOException {
            if (this.settings == null)
                return;
            if (this.manager != null)
                this.manager.close();
            if (this.taxoWriter != null)
                this.taxoWriter.close();
            if (this.indexWriter != null)
                this.indexWriter.close();
        }

        public void initSettings(Path stateDir, LuceneSettings settings) throws Exception {
            if (this.settings != null)
                throw new Exception("Init settings is only allowed once");
            this.settings = settings;

            MMapDirectory indexDirectory = new MMapDirectory(stateDir.resolve("index"));
            indexDirectory.setUseUnmap(false);

            MMapDirectory taxoDirectory = new MMapDirectory(stateDir.resolve("taxo"));
            taxoDirectory.setUseUnmap(false);

            IndexWriterConfig config = new IndexWriterConfig(settings.analyzer);
            config.setSimilarity(settings.similarity);
            config.setMergePolicy(settings.getMergePolicy());

            this.indexWriter = new IndexWriter(indexDirectory, config);
            this.indexWriter.commit();
            this.taxoWriter = new DirectoryTaxonomyWriter(taxoDirectory, IndexWriterConfig.OpenMode.CREATE_OR_APPEND, new LruTaxonomyWriterCache(settings.lruTaxonomyWriterCacheSize));
            this.taxoWriter.commit();

            this.scoreCollectorCache = Collections.synchronizedMap(new LRUMap<KeyNameQuery, ScoreSuperCollector>(50));
            this.keyCollectorCache = Collections.synchronizedMap(new LRUMap<KeyNameQuery, FixedBitSet>(50));

            this.manager = new SearcherTaxonomyManager(indexDirectory, taxoDirectory, new MerescoSearchFactory(indexDirectory, taxoDirectory, settings));
            this.manager.addListener(refreshListener);
        }

        public IndexWriter getIndexWriter() throws UninitializedException {
            if (this.settings == null)
                throw new UninitializedException();
            return indexWriter;
        }

        public DirectoryTaxonomyWriter getTaxoWriter() throws UninitializedException {
            if (this.settings == null)
                throw new UninitializedException();
            return taxoWriter;
        }

        public FacetsConfig getFacetsConfig() throws UninitializedException {
            if (this.settings == null)
                throw new UninitializedException();
            return this.settings.facetsConfig;
        }

        public LuceneSettings getSettings() throws UninitializedException {
            if (this.settings == null)
                throw new UninitializedException();
            return settings;
        }

        public boolean hasSettings() {
            return this.settings != null;
        }

        public Map<KeyNameQuery, ScoreSuperCollector> getScoreCollectorCache() throws UninitializedException {
            if (this.settings == null)
                throw new UninitializedException();
            return scoreCollectorCache;
        }

        public Map<KeyNameQuery, FixedBitSet> getKeyCollectorCache() throws UninitializedException {
            if (this.settings == null)
                throw new UninitializedException();
            return keyCollectorCache;
        }

        public SearcherTaxonomyManager getManager() throws UninitializedException {
            if (this.settings == null)
                throw new UninitializedException();
            return manager;
        }

        private class LuceneRefreshListener implements RefreshListener {
            private boolean refreshed;

            @Override
            public void afterRefresh(boolean didRefresh) throws IOException {
                if (!refreshed)
                    refreshed = didRefresh;
            }

            @Override
            public void beforeRefresh() throws IOException {
            }

            public boolean isRefreshed() {
                if (refreshed) {
                    refreshed = false;
                    return true;
                }
                return false;
            }
        }
    }
}

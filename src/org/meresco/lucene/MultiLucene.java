/* begin license *
 *
 * "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
 *
 * Copyright (C) 2015 Koninklijke Bibliotheek (KB) http://www.kb.nl
 * Copyright (C) 2015-2016 Seecr (Seek You Too B.V.) http://seecr.nl
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.FixedBitSet;
import org.meresco.lucene.ComposedQuery.Unite;
import org.meresco.lucene.QueryConverter.FacetRequest;
import org.meresco.lucene.queries.KeyFilter;
import org.meresco.lucene.search.JoinSortCollector;
import org.meresco.lucene.search.JoinSortField;
import org.meresco.lucene.search.SuperCollector;
import org.meresco.lucene.search.join.AggregateScoreSuperCollector;
import org.meresco.lucene.search.join.KeySuperCollector;
import org.meresco.lucene.search.join.ScoreSuperCollector;


public class MultiLucene {
    private Map<String, Lucene> lucenes = new HashMap<String, Lucene>();

    public MultiLucene(List<Lucene> lucenes) {
        for (Lucene lucene : lucenes) {
            this.lucenes.put(lucene.name, lucene);
        }
    }

    public LuceneResponse executeComposedQuery(ComposedQuery q) throws Throwable {
        return executeComposedQuery(q, null);
    }

    public LuceneResponse executeComposedQuery(ComposedQuery q, String exportKey) throws Throwable {
        if (q.cores.size() <= 1 && exportKey == null)
            return singleCoreQuery(q);
        return multipleCoreQuery(q, exportKey);
    }

    private LuceneResponse singleCoreQuery(ComposedQuery query) throws Throwable {
        String resultCoreName = query.resultsFrom;
        Query resultCoreQuery = luceneQueryForCore(resultCoreName, query);
        if (resultCoreQuery == null)
            resultCoreQuery = new MatchAllDocsQuery();
        query.queryData.query = resultCoreQuery;
        query.queryData.facets = query.facetsFor(resultCoreName);
        return this.lucenes.get(resultCoreName).executeQuery(query.queryData, query.filterQueries.get(resultCoreName), query.drilldownQueriesFor(resultCoreName), null, null, null);
    }

    private LuceneResponse multipleCoreQuery(ComposedQuery query, String exportKey) throws Throwable {
        long t0 = System.currentTimeMillis();
        String resultCoreName = query.resultsFrom;
        List<String> otherCoreNames = new ArrayList<>();
        for (String core : query.cores)
            if (!core.equals(resultCoreName))
                otherCoreNames.add(core);

        Map<String, FixedBitSet> finalKeys = uniteFilter(query);
        for (String otherCoreName : otherCoreNames)
            finalKeys = coreQueries(otherCoreName, resultCoreName, query, finalKeys);

        List<Query> resultFilters = new ArrayList<>();
        for (String keyName : finalKeys.keySet())
            resultFilters.add(new KeyFilter(finalKeys.get(keyName), keyName));

        Query resultCoreQuery = luceneQueryForCore(resultCoreName, query);
        if (resultCoreQuery == null)
                resultCoreQuery = new MatchAllDocsQuery();
        List<AggregateScoreSuperCollector> aggregateScoreCollectors = createAggregateScoreCollectors(query);
        Map<String, KeySuperCollector> keyCollectors = new HashMap<>();
        for (String keyName : query.keyNames(resultCoreName)) {
            keyCollectors.put(keyName, new KeySuperCollector(keyName));
        }
        if (exportKey != null && !keyCollectors.containsKey(exportKey)) {
            keyCollectors.put(exportKey, new KeySuperCollector(exportKey));
        }

        if (query.queryData.sort != null) {
            SortField[] sortFields = query.queryData.sort.getSort();
            for (int i=0; i<sortFields.length; i++) {
                if (!(sortFields[i] instanceof JoinSortField))
                    continue;
                JoinSortField sortField = (JoinSortField) sortFields[i];
                String otherCoreName = sortField.getCoreName();
                JoinSortCollector collector = new JoinSortCollector(query.keyName(resultCoreName, otherCoreName), query.keyName(otherCoreName, resultCoreName));
                this.lucenes.get(otherCoreName).search(new MatchAllDocsQuery(), null, collector);
                sortField.setCollector(collector);
            }
        }

        query.queryData.query = resultCoreQuery;
        query.queryData.facets = query.facetsFor(resultCoreName);
        LuceneResponse response = this.lucenes.get(resultCoreName).executeQuery(
                query.queryData,
                null, // TODO: filterQueries??
                query.drilldownQueriesFor(resultCoreName),
                resultFilters,
                aggregateScoreCollectors,
                keyCollectors.values()
            );

        for (String otherCoreName : otherCoreNames) {
            List<FacetRequest> facets = query.facetsFor(otherCoreName);
            if (facets != null && facets.size() > 0) {
                String coreKey = query.keyName(resultCoreName, otherCoreName);
                KeyFilter keyFilter = new KeyFilter(keyCollectors.get(coreKey).getCollectedKeys(), query.keyName(otherCoreName, resultCoreName));
                List<Query> queries = new ArrayList<Query>();
                queries.addAll(query.queriesFor(otherCoreName));
                queries.addAll(query.otherCoreFacetFiltersFor(otherCoreName));
                response.drilldownData.addAll(lucenes.get(otherCoreName).facets(
                        facets,
                        queries,
                        query.drilldownQueriesFor(otherCoreName),
                        keyFilter
                    ));
            }
        }

        if (exportKey != null) {
            response.keys = keyCollectors.get(exportKey).getCollectedKeys();
        }
        response.queryTime = System.currentTimeMillis() - t0;
        return response;
    }

    private Map<String, FixedBitSet> uniteFilter(ComposedQuery query) throws Throwable {
        Map<String, FixedBitSet> keys = new HashMap<String, FixedBitSet>();
        for (Unite unite : query.getUnites()) {
            String keyNameA = query.keyName(unite.coreA, unite.coreB);
            String keyNameB = query.keyName(unite.coreB, unite.coreA);
            String resultKeyName = query.resultsFrom.equals(unite.coreA) ? keyNameA : keyNameB;
            FixedBitSet collectedKeys = lucenes.get(unite.coreA).collectKeys(unite.queryA, query.keyName(unite.coreA, unite.coreB), null);
            unionCollectedKeys(keys, collectedKeys, resultKeyName);

            collectedKeys = lucenes.get(unite.coreB).collectKeys(unite.queryB, query.keyName(unite.coreB, unite.coreA), null);
            unionCollectedKeys(keys, collectedKeys, resultKeyName);
        }

        for (String core : query.filterQueries.keySet()) {
            for (Query q : query.filterQueries.get(core)) {
                String keyNameResult = query.keyName(query.resultsFrom, core);
                String keyNameOther = query.keyName(core, query.resultsFrom);
                FixedBitSet collectedKeys = lucenes.get(core).collectKeys(q, keyNameOther, null);
                if (keys.containsKey(keyNameResult))
                    keys.get(keyNameResult).and(collectedKeys);
                else
                    keys.put(keyNameResult, collectedKeys.clone());
            }
        }
        return keys;
    }

    private void unionCollectedKeys(Map<String, FixedBitSet> keys, FixedBitSet collectedKeys, String keyName) {
        if (keys.containsKey(keyName)) {
            FixedBitSet bitSet = FixedBitSet.ensureCapacity(keys.get(keyName), collectedKeys.length());
            bitSet.or(collectedKeys);
            keys.put(keyName, bitSet);
        } else
            keys.put(keyName, collectedKeys.clone());
    }

    private Query luceneQueryForCore(String coreName, ComposedQuery query) throws Exception {
        Query luceneQuery = query.queryFor(coreName);
        List<String[]> ddQueries = query.drilldownQueriesFor(coreName);
        if (ddQueries != null && ddQueries.size() > 0)
            luceneQuery = lucenes.get(coreName).createDrilldownQuery(luceneQuery, ddQueries);
        return luceneQuery;
    }

    private Map<String, FixedBitSet> coreQueries(String coreName, String otherCoreName, ComposedQuery query, Map<String, FixedBitSet> keysForKeyName) throws Throwable {
        Query luceneQuery = luceneQueryForCore(coreName, query);
        if (luceneQuery != null) {
            FixedBitSet collectedKeys = this.lucenes.get(coreName).collectKeys(null, query.keyName(coreName, otherCoreName), luceneQuery, false);
            String otherKeyName = query.keyName(otherCoreName, coreName);
            if (keysForKeyName.containsKey(otherKeyName))
                keysForKeyName.get(otherKeyName).and(collectedKeys);
            else
                keysForKeyName.put(otherKeyName, collectedKeys);
        }
        return keysForKeyName;
    }

    public Map<String, QueryConverter> getQueryConverters() throws Exception {
        Map<String, QueryConverter> queryConverters = new HashMap<String, QueryConverter>();
        for (Lucene lucene : this.lucenes.values())
            queryConverters.put(lucene.name, lucene.getQueryConverter());
        return queryConverters;
    }

    public List<AggregateScoreSuperCollector> createAggregateScoreCollectors(ComposedQuery query) throws Throwable {
        Map<String, List<ScoreSuperCollector>> scoreCollectors = new HashMap<String, List<ScoreSuperCollector>>();
        for (String coreName : query.cores) {
            String resultsKeyName = query.keyName(query.resultsFrom, coreName);
            Query rankQuery = query.rankQueryFor(coreName);
            if (rankQuery != null) {
                ScoreSuperCollector scoreCollector = this.lucenes.get(coreName).scoreCollector(query.keyName(coreName, query.resultsFrom), rankQuery);
                if (!scoreCollectors.containsKey(resultsKeyName))
                    scoreCollectors.put(resultsKeyName, new ArrayList<ScoreSuperCollector>());
                scoreCollectors.get(resultsKeyName).add(scoreCollector);
            }
        }
        List<AggregateScoreSuperCollector> aggregateScoreCollectors = new ArrayList<AggregateScoreSuperCollector>();
        for (String keyName : scoreCollectors.keySet()) {
            List<ScoreSuperCollector> scoreCollectorsForKey = scoreCollectors.get(keyName);
            if (scoreCollectorsForKey.size() > 0)
                aggregateScoreCollectors.add(new AggregateScoreSuperCollector(keyName, scoreCollectorsForKey));
        }
        return aggregateScoreCollectors;
    }
}


/* begin license *
 *
 * "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
 *
 * Copyright (C) 2015-2016 Koninklijke Bibliotheek (KB) http://www.kb.nl
 * Copyright (C) 2015-2016, 2018-2020 Seecr (Seek You Too B.V.) https://seecr.nl
 * Copyright (C) 2016, 2020 Stichting Kennisnet https://www.kennisnet.nl
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

import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonString;
import javax.json.JsonValue;

import org.apache.lucene.search.Query;
import org.meresco.lucene.JsonQueryConverter.FacetRequest;


public class ComposedQuery {
    public String resultsFrom;
    public Set<String> cores = new HashSet<String>();
    private Map<String, Query> queries = new HashMap<String, Query>();
    public Query relationalFilter;
    public Map<String, List<Query>> filterQueries = new HashMap<String, List<Query>>();
    public Map<String, List<Query>> excludeFilterQueries = new HashMap<String, List<Query>>();
    private Map<String, List<FacetRequest>> facets = new HashMap<String, List<FacetRequest>>();
    private Map<String, List<String[]>> drilldownQueries = new HashMap<String, List<String[]>>();
    private Map<String, List<Query>> otherCoreFacetsFilter = new HashMap<String, List<Query>>();
    private Map<String, Query> rankQueries = new HashMap<String, Query>();
    public Float rankQueryScoreRatio;
    private List<Unite> unites = new ArrayList<Unite>();
    public List<Match> matches = new ArrayList<Match>();
    public QueryData queryData = new QueryData();
    public ClusterConfig clusterConfig;

    public String toString() {
        return "ComposedQuery(resultsFrom=" + resultsFrom + ",\n    " +
            "cores=" + cores + ",\n    " +
            "queries=" + queries + ",\n    " +
            "relationalFilter=" + relationalFilter + ",\n    " +
            "filterQueries=" + filterQueries + ",\n    " +
            "excludeFilterQueries=" + excludeFilterQueries + ",\n    " +
            "facets=" + facets + ",\n    " +
            "drilldownQueries=" + drilldownQueries + ",\n    " +
            "otherCoreFacetsFilter=" + otherCoreFacetsFilter + ",\n    " +
            "rankQueries=" + rankQueries + ",\n    " +
            "rankQueryScoreRatio=" + rankQueryScoreRatio + ",\n    " +
            "unites=" + unites + ",\n    " +
            "matches=" + matches + ",\n    " +
            "queryData=" + queryData + ",\n    " +
            "clusterConfig=" + clusterConfig + ")";
    }

    public ComposedQuery(String resultsFrom) {
        this.resultsFrom = resultsFrom;
    }

    public ComposedQuery(String resultsFrom, Query query) {
        this.resultsFrom = resultsFrom;
        this.queries.put(resultsFrom, query);
    }

    public static ComposedQuery fromJsonString(Reader jsonStringReader, Map<String, JsonQueryConverter> queryConverters) {
        if (jsonStringReader == null) {
            return null;
        }
        JsonObject json = Json.createReader(jsonStringReader).readObject();
        String resultsFrom = json.getString("resultsFrom");
        ComposedQuery cq = new ComposedQuery(resultsFrom);
        if (json.containsKey("_start") && json.get("_start") != JsonValue.NULL) {
            cq.queryData.start = json.getInt("_start");
        }
        if (json.containsKey("_stop") && json.get("_stop") != JsonValue.NULL) {
            cq.queryData.stop = json.getInt("_stop");
        }
        JsonQueryConverter queryConverter = queryConverters.get(resultsFrom);
        if (json.containsKey("_suggestionRequest") && json.get("_suggestionRequest") != JsonValue.NULL) {
            cq.queryData.suggestionRequest = queryConverter.convertToSuggestionRequest(json.getJsonObject("_suggestionRequest"));
        }
        cq.queryData.sort = queryConverter.convertToSort(json.getJsonArray("_sortKeys"));
        cq.queryData.dedupField = json.getString("_dedupField", null);
        cq.queryData.getDedupSortFieldsFromJson(json, "_dedupSortField");
        cq.queryData.clustering = json.getBoolean("_clustering", false);
        JsonArray fields = json.getJsonArray("_storedFields");
        if (fields != null) {
            fields.stream().forEach(s -> cq.queryData.storedFields.add(((JsonString) s).getString()));
        }
        if (json.containsKey("_clusteringConfig")) {
        	cq.queryData.clusterConfig = ClusterConfig.parseFromJsonObject(json.getJsonObject("_clusteringConfig"));
        }
        if (json.containsKey("cores")) {
            JsonArray jsonCores = json.getJsonArray("cores");
            for (int i = 0; i < jsonCores.size(); i++) {
                cq.cores.add(jsonCores.getString(i));
            }
        }
        if (json.containsKey("_queries")) {
            JsonObject queries = json.getJsonObject("_queries");
            for (String core : queries.keySet()) {
                cq.setCoreQuery(core, queryConverters.get(core).convertToQuery(queries.getJsonObject(core)));
            }
        }
        if (json.containsKey("_relationalFilterJson")) {
            String relationalFilterJson = json.getString("_relationalFilterJson");
            StringReader reader = new StringReader(relationalFilterJson);
            JsonObject relationalFilterJsonObject = Json.createReader(reader).readObject();
            reader.close();
            cq.relationalFilter = queryConverters.get(resultsFrom).convertToQuery(relationalFilterJsonObject);
        }
        if (json.containsKey("_filterQueries")) {
            JsonObject filterQueries = json.getJsonObject("_filterQueries");
            for (String coreName : filterQueries.keySet()) {
                JsonArray queries = filterQueries.getJsonArray(coreName);
                for (int i = 0; i < queries.size(); i++) {
                    cq.addFilterQuery(coreName, queryConverters.get(coreName).convertToQuery(queries.getJsonObject(i)));
                }
            }
        }
        if (json.containsKey("_excludeFilterQueries")) {
            JsonObject excludeFilterQueries = json.getJsonObject("_excludeFilterQueries");
            for (String coreName : excludeFilterQueries.keySet()) {
                JsonArray queries = excludeFilterQueries.getJsonArray(coreName);
                for (int i = 0; i < queries.size(); i++) {
                    cq.addExcludeFilterQuery(coreName, queryConverters.get(coreName).convertToQuery(queries.getJsonObject(i)));
                }
            }
        }
        if (json.containsKey("_otherCoreFacetFilters")) {
            JsonObject filterQueries = json.getJsonObject("_otherCoreFacetFilters");
            for (String coreName : filterQueries.keySet()) {
                JsonArray queries = filterQueries.getJsonArray(coreName);
                for (int i = 0; i < queries.size(); i++) {
                    cq.addOtherCoreFacetFilter(coreName, queryConverters.get(coreName).convertToQuery(queries.getJsonObject(i)));
                }
            }
        }
        if (json.containsKey("_drilldownQueries")) {
            JsonObject drilldownQueries = json.getJsonObject("_drilldownQueries");
            for (String coreName : drilldownQueries.keySet()) {
                JsonArray ddQueries = drilldownQueries.getJsonArray(coreName);
                for (int i = 0; i < ddQueries.size(); i++) {
                    JsonArray ddQuery = ddQueries.getJsonArray(i);
                    JsonArray jsonPath = ddQuery.getJsonArray(1);
                    String[] path = new String[jsonPath.size()];
                    for (int j=0; j < jsonPath.size(); j++) {
                        path[j] = jsonPath.getString(j);
                    }
                    cq.addDrilldownQuery(coreName, ddQuery.getString(0), path);
                }
            }
        }
        if (json.containsKey("_facets")) {
            JsonObject jsonFacets = json.getJsonObject("_facets");
            for (String coreName : jsonFacets.keySet()) {
                for (FacetRequest facetRequest : queryConverters.get(coreName).convertToFacets(jsonFacets.getJsonArray(coreName))) {
                    cq.addFacet(coreName, facetRequest);
                }
            }
        }
        if (json.containsKey("_matches")) {
            JsonObject matches = json.getJsonObject("_matches");
            for (String match : matches.keySet()) {
                String[] coreNames = match.split("->");
                JsonArray coreDicts = matches.getJsonArray(match);
                JsonObject coreSpec1 = coreDicts.getJsonObject(0);
                JsonObject coreSpec2 = coreDicts.getJsonObject(1);
                String keyName1 = coreSpec1.getString("key", coreSpec1.getString("uniqueKey", null));
                String keyName2 = coreSpec2.getString("key", coreSpec2.getString("uniqueKey", null));
                if (coreSpec1.getString("core").equals(coreNames[0])) {
                    cq.addMatch(coreNames[0], coreNames[1], keyName1, keyName2);
                } else {
                    cq.addMatch(coreNames[0], coreNames[1], keyName2, keyName1);
                }
            }
        }
        if (json.containsKey("_rankQueries")) {
            JsonObject rankQueries = json.getJsonObject("_rankQueries");
            for (String coreName : rankQueries.keySet()) {
                cq.setRankQuery(coreName, queryConverters.get(coreName).convertToQuery(rankQueries.getJsonObject(coreName)));
            }
        }
        if (json.containsKey("_rankQueryScoreRatio")) {
            cq.rankQueryScoreRatio = (float) json.getJsonNumber("_rankQueryScoreRatio").doubleValue();
        }
        if (json.containsKey("_unites")) {
            JsonArray unites = json.getJsonArray("_unites");
            for (int i = 0; i < unites.size(); i++) {
                JsonArray A = unites.getJsonObject(i).getJsonArray("A");
                JsonArray B = unites.getJsonObject(i).getJsonArray("B");
                String core1 = A.getString(0);
                Query query1 = queryConverters.get(core1).convertToQuery(A.getJsonObject(1));
                String core2 = B.getString(0);
                Query query2 = queryConverters.get(core2).convertToQuery(B.getJsonObject(1));
                cq.addUnite(core1, query1, core2, query2);
            }
        }
        return cq;
    }

    public void addMatch(String core1, String core2, String core1Key, String core2Key) {
        matches.add(new Match(core1, core2, core1Key));
        matches.add(new Match(core2, core1, core2Key));
        cores.add(core1);
        cores.add(core2);
    }

    public Query queryFor(String core) {
        return this.queries.get(core);
    }

    public List<Query> queriesFor(String core) {
        List<Query> qs = new ArrayList<Query>();
        Query otherCoreQuery = queryFor(core);
        if (otherCoreQuery != null)
            qs.add(otherCoreQuery);
        List<Query> otherCoreFilterQueries = filterQueries.get(core);
        if (otherCoreFilterQueries != null)
            qs.addAll(otherCoreFilterQueries);
        return qs;
    }

    public List<FacetRequest> facetsFor(String core) {
        if (!this.facets.containsKey(core))
            return new ArrayList<FacetRequest>();
        return this.facets.get(core);
    }

    public List<String[]> drilldownQueriesFor(String core) {
        return this.drilldownQueries.get(core);
    }

    public List<Query> otherCoreFacetFiltersFor(String core) {
        if (!this.otherCoreFacetsFilter.containsKey(core))
            return new ArrayList<Query>();
        return this.otherCoreFacetsFilter.get(core);
    }

    public Query rankQueryFor(String core) {
        return this.rankQueries.get(core);
    }

    public List<Unite> getUnites() {
        return this.unites;
    }

    public String keyName(String coreName, String otherCoreName) {
        for (Match match : matches) {
            if (match.core1.equals(coreName) && match.core2.equals(otherCoreName))
                return match.keyName;
            else if (coreName.equals(otherCoreName) && match.core1.equals(coreName))
                return match.keyName;
        }
        return null;
    }

    public String[] keyNames(String core) {
        Set<String> keyNames = new HashSet<String>();
        for (String coreName : this.cores) {
            if (!coreName.equals(core)) {
                keyNames.add(keyName(core, coreName));
            }
        }
        return keyNames.toArray(new String[0]);
    }

    public void setCoreQuery(String coreName, Query query) {
        this.queries.put(coreName, query);
    }

    public void addFilterQuery(String coreName, Query query) {
        if (filterQueries.containsKey(coreName))
            filterQueries.get(coreName).add(query);
        else {
            List<Query> queries = new ArrayList<Query>();
            queries.add(query);
            filterQueries.put(coreName, queries);
        }
    }

    public void addExcludeFilterQuery(String coreName, Query query) {
        if (excludeFilterQueries.containsKey(coreName))
            excludeFilterQueries.get(coreName).add(query);
        else {
            List<Query> queries = new ArrayList<Query>();
            queries.add(query);
            excludeFilterQueries.put(coreName, queries);
        }
    }

    public void addDrilldownQuery(String coreName, String dim, String... value) {
        if (drilldownQueries.containsKey(coreName)) {
            drilldownQueries.get(coreName).add(new String[] { dim });
            drilldownQueries.get(coreName).add(value);
        } else {
            List<String[]> fieldValuesMap = new ArrayList<String[]>();
            fieldValuesMap.add(new String[] { dim });
            fieldValuesMap.add(value);
            drilldownQueries.put(coreName, fieldValuesMap);
        }
    }

    public void addOtherCoreFacetFilter(String coreName, Query query) {
        if (this.otherCoreFacetsFilter.containsKey(coreName))
            this.otherCoreFacetsFilter.get(coreName).add(query);
        else {
            List<Query> otherCoreFacetsFilter = new ArrayList<Query>();
            otherCoreFacetsFilter.add(query);
            this.otherCoreFacetsFilter.put(coreName, otherCoreFacetsFilter);
        }
    }

    public void addUnite(String core1, Query query1, String core2, Query query2) {
        this.unites.add(new Unite(core1, core2, query1, query2));
    }

    public void addFacet(String coreName, FacetRequest facetRequest) {
        if (this.facets.containsKey(coreName))
            this.facets.get(coreName).add(facetRequest);
        else {
            List<FacetRequest> facets = new ArrayList<FacetRequest>();
            facets.add(facetRequest);
            this.facets.put(coreName, facets);
        }
    }

    public void setRankQuery(String core, Query query) {
        this.rankQueries.put(core, query);
    }

    static class Match {
        private String core1;
        private String core2;
        private String keyName;

        public String toString() {
            return "Match(" + core1 + ", " + core2 + ", " + keyName + ")";
        }

        public Match(String core1, String core2, String keyName) {
            this.core1 = core1;
            this.core2 = core2;
            this.keyName = keyName;
        }
    }

    static class Unite {
        public String coreA;
        public String coreB;
        public Query queryA;
        public Query queryB;

        public String toString() {
            return "Unite(" + coreA + ", " + coreB + ", " + queryA + ", " + queryB + ")";
        }

        public Unite(String core1, String core2, Query query1, Query query2) {
            this.coreA = core1;
            this.coreB = core2;
            this.queryA = query1;
            this.queryB = query2;
        }
    }
}

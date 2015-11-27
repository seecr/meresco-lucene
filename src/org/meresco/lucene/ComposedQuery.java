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

import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonValue;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.meresco.lucene.QueryConverter.FacetRequest;

public class ComposedQuery {

    public String resultsFrom;
    public Set<String> cores = new HashSet<String>();
    private Map<String, Query> queries = new HashMap<String, Query>();
    public Map<String, List<Query>> filterQueries = new HashMap<String, List<Query>>();
    private Map<String, List<FacetRequest>> facets = new HashMap<String, List<FacetRequest>>();
    private Map<String, Map<String, String>> drilldownQueries = new HashMap<String, Map<String, String>>();
    private Map<String, List<Query>> otherCoreFacetsFilter = new HashMap<String, List<Query>>();
    private List<Unite> unites = new ArrayList<Unite>();
    public int start = 0;
    public int stop = 10;
    public Sort sort;
    public List<Match> matches = new ArrayList<Match>();

    public ComposedQuery(String resultsFrom) {
        this.resultsFrom = resultsFrom;
    }

    public ComposedQuery(String resultsFrom, Query query) {
        this.resultsFrom = resultsFrom;
        this.queries.put(resultsFrom, query);
    }

    public static ComposedQuery fromJsonString(Reader jsonStringReader, Map<String, QueryConverter> converters) {
        if (jsonStringReader == null)
            return null;
        JsonObject json = Json.createReader(jsonStringReader).readObject();
        ComposedQuery cq = new ComposedQuery(json.getString("resultsFrom"));
        if (json.containsKey("start") && json.get("start") != JsonValue.NULL)
            cq.start = json.getInt("start");
        if (json.containsKey("stop") && json.get("stop") != JsonValue.NULL)
            cq.stop = json.getInt("stop");
        if (json.containsKey("cores")) {
            JsonArray jsonCores = json.getJsonArray("cores");
            for (int i = 0; i < jsonCores.size(); i++) {
                cq.cores.add(jsonCores.getString(i));
            }
        }
        if (json.containsKey("queries")) {
            JsonObject queries = json.getJsonObject("queries");
            for (String core : queries.keySet()) {
                cq.setCoreQuery(core, converters.get(core).convertToQuery(queries.getJsonObject(core)));
            }
        }
        if (json.containsKey("filterQueries")) {
            JsonObject filterQueries = json.getJsonObject("filterQueries");
            for (String coreName : filterQueries.keySet()) {
                JsonArray queries = filterQueries.getJsonArray(coreName);
                for (int i = 0; i < queries.size(); i++) {
                    cq.addFilterQuery(coreName, converters.get(coreName).convertToQuery(queries.getJsonObject(i)));
                }
            }
        }
        if (json.containsKey("otherCoreFacetFilters")) {
            JsonObject filterQueries = json.getJsonObject("otherCoreFacetFilters");
            for (String coreName : filterQueries.keySet()) {
                JsonArray queries = filterQueries.getJsonArray(coreName);
                for (int i = 0; i < queries.size(); i++) {
                    cq.addOtherCoreFacetFilter(coreName, converters.get(coreName).convertToQuery(queries.getJsonObject(i)));
                }
            }
        }
        if (json.containsKey("drilldownQueries")) {
            JsonObject filterQueries = json.getJsonObject("drilldownQueries");
            for (String coreName : filterQueries.keySet()) {
                JsonArray ddQueries = filterQueries.getJsonArray(coreName);
                for (int i = 0; i < ddQueries.size(); i++) {
                    JsonArray ddQuery = ddQueries.getJsonArray(i);
                    cq.addDrilldownQuery(coreName, ddQuery.getString(0), ddQuery.getString(1));
                }
            }
        }
        cq.sort = new QueryConverter(null).convertToSort(json.getJsonArray("sortKeys"));
        if (json.containsKey("facets")) {
            JsonObject jsonFacets = json.getJsonObject("facets");
            for (String coreName : jsonFacets.keySet()) {
                for (FacetRequest facetRequest : converters.get(coreName).convertToFacets(jsonFacets.getJsonArray(coreName)))
                    cq.addFacet(coreName, facetRequest);
            }
        }

        if (json.containsKey("matches")) {
            JsonObject matches = json.getJsonObject("matches");
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

    public Map<String, String> drilldownQueriesFor(String core) {
        return this.drilldownQueries.get(core);
    }
    
    public List<Query> otherCoreFacetFiltersFor(String core) {
        if (!this.otherCoreFacetsFilter.containsKey(core))
            return new ArrayList<Query>();
        return this.otherCoreFacetsFilter.get(core);
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
            if (!coreName.equals(core))
                keyNames.add(keyName(core, coreName));
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

    public void addDrilldownQuery(String coreName, String dim, String value) {
        if (drilldownQueries.containsKey(coreName))
            drilldownQueries.get(coreName).put(dim, value);
        else {
            Map<String, String> fieldValuesMap = new HashMap<String, String>();
            fieldValuesMap.put(dim, value);
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

    static class Match {
        private String core1;
        private String core2;
        private String keyName;

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

        public Unite(String core1, String core2, Query query1, Query query2) {
            this.coreA = core1;
            this.coreB = core2;
            this.queryA = query1;
            this.queryB = query2;
        }
    }
}

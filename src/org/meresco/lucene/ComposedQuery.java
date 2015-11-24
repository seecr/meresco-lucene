package org.meresco.lucene;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonValue;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TermQuery;
import org.meresco.lucene.QueryStringToQuery.FacetRequest;

public class ComposedQuery {

    public String resultsFrom;
    public Set<String> cores = new HashSet<String>();
    private Map<String, Query> queries = new HashMap<String, Query>();
    public Map<String, List<Query>> filterQueries = new HashMap<String, List<Query>>();
    public List<Unite> unites = new ArrayList<Unite>();
    private Map<String, List<FacetRequest>> facets = new HashMap<String, List<FacetRequest>>();
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

    public static ComposedQuery fromJson(QueryStringToQuery queryStringToQuery, JsonObject json) {
        if (json == null)
            return null;
        ComposedQuery cq = new ComposedQuery(json.getString("resultsFrom"));
        cq.start = json.getInt("start");
        cq.stop = json.getInt("stop");
        JsonArray jsonCores = json.getJsonArray("cores");
        for (int i = 0; i < jsonCores.size(); i++) {
            cq.cores.add(jsonCores.getString(i)); 
        }
        JsonObject queries = json.getJsonObject("queries");
        for (String core : queries.keySet()) {
            cq.setCoreQuery(core, queryStringToQuery.convertToQuery(queries.getJsonObject(core)));
        }
        cq.sort = queryStringToQuery.convertToSort(json.getJsonArray("sortKeys"));
        
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
        return this.facets.get(core);
    }
    
    public String[] drilldownQueriesFor(String core) {
        return null;
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

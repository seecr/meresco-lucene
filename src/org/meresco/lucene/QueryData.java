package org.meresco.lucene;

import java.io.Reader;
import java.util.List;

import javax.json.Json;
import javax.json.JsonObject;

import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.meresco.lucene.QueryConverter.FacetRequest;
import org.meresco.lucene.QueryConverter.SuggestionRequest;

public class QueryData {
    public Query query = new MatchAllDocsQuery();
    public List<FacetRequest> facets;
    public int start = 0;
    public int stop = 10;
    public Sort sort;
    public SuggestionRequest suggestionRequest;
    public String dedupField;
    public String dedupSortField;
    public String groupingField;

    public QueryData(Reader queryReader, QueryConverter converter) {
        JsonObject object = Json.createReader(queryReader).readObject();
        this.query = converter.convertToQuery(object.getJsonObject("query"));
        this.facets = converter.convertToFacets(object.getJsonArray("facets"));
        this.start = object.getInt("start", 0);
        this.stop = object.getInt("stop", 10);
        this.sort = converter.convertToSort(object.getJsonArray("sortKeys"));
        this.suggestionRequest = converter.convertToSuggestionRequest(object.getJsonObject("suggestionRequest"));
        this.dedupField = object.getString("dedupField", null);
        this.dedupSortField = object.getString("dedupSortField", null);
        this.groupingField = object.getString("groupingField", null);
    }

    public QueryData() {
        // TODO Auto-generated constructor stub
    }
}
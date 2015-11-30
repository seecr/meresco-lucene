package org.meresco.lucene;

import java.io.Reader;
import java.util.List;

import javax.json.Json;
import javax.json.JsonObject;

import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.meresco.lucene.QueryConverter.FacetRequest;
import org.meresco.lucene.QueryConverter.SuggestionRequest;

public class QueryData {
    public Query query;
    public List<FacetRequest> facets;
    public int start;
    public int stop;
    public Sort sort;
    public SuggestionRequest suggestionRequest;

    public QueryData(Reader queryReader, QueryConverter converter) {
        JsonObject object = Json.createReader(queryReader).readObject();
        this.query = converter.convertToQuery(object.getJsonObject("query"));
        this.facets = converter.convertToFacets(object.getJsonArray("facets"));
        this.start = object.getInt("start", 0);
        this.stop = object.getInt("stop", 10);
        this.sort = converter.convertToSort(object.getJsonArray("sortKeys"));
        this.suggestionRequest = converter.convertToSuggestionRequest(object.getJsonObject("suggestionRequest"));
    }
}
package org.meresco.lucene;

import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;

public class QueryStringToQuery {
    
    public Query query;
    public List<FacetRequest> facets;

    public QueryStringToQuery(Reader queryReader) {
        JsonObject object = Json.createReader(queryReader).readObject();
        this.query = convertToQuery(object.getJsonObject("query"));
        this.facets = convertToFacets(object.getJsonArray("facets"));
    }
    
    private List<FacetRequest> convertToFacets(JsonArray facets) {
        if (facets == null)
            return null;
        List<FacetRequest> facetRequests = new ArrayList<FacetRequest>();
        for (int i = 0; i < facets.size(); i++) {
            JsonObject facet = facets.getJsonObject(i);
            FacetRequest fr = new FacetRequest(facet.getString("fieldname"), facet.getInt("maxTerms"));
            facetRequests.add(fr);
        }
        return facetRequests;
    }

    public Query convertToQuery(JsonObject query) {
        switch(query.getString("type")) {
            case "MatchAllDocsQuery":
                return new MatchAllDocsQuery();
            case "TermQuery":
                return createTermQuery(query.getJsonObject("term"));
            default:
                return null;
        }
    }

    private Query createTermQuery(JsonObject object) {
        return new TermQuery(new Term(object.getString("field"), object.getString("value")));
    }
    
    public static class FacetRequest {
        public String fieldname;
        public int maxTerms;
        public String[] path = new String[0];

        public FacetRequest(String fieldname, int maxTerms) {
            this.fieldname = fieldname;
            this.maxTerms = maxTerms;
        }
    }
   
}

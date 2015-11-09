package org.meresco.lucene;

import java.io.StringReader;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;

import javax.json.*;

public class QueryStringToQuery {
    
    private JsonObject object;

    public QueryStringToQuery(String query) {
        JsonReader jsonReader = Json.createReader(new StringReader(query));
        object = jsonReader.readObject();
    }
    
    public Query convert() {
        switch(object.getString("type")) {
            case "MatchAllDocsQuery":
                return new MatchAllDocsQuery();
            case "TermQuery":
                return createTermQuery(object.getJsonObject("term"));
            default:
                return null;
        }
    }

    private Query createTermQuery(JsonObject object) {
        return new TermQuery(new Term(object.getString("field"), object.getString("value")));
    }
}

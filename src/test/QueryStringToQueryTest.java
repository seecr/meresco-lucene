package test;

import static org.junit.Assert.assertEquals;

import java.io.StringReader;

import javax.json.Json;
import javax.json.JsonObject;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.junit.Test;
import org.meresco.lucene.QueryStringToQuery;

public class QueryStringToQueryTest {

    @Test
    public void testTermQuery() {
        JsonObject json = Json.createObjectBuilder()
                .add("type", "TermQuery")
                .add("term", Json.createObjectBuilder()
                    .add("field", "field")
                    .add("value", "value"))
                .build();
        assertEquals(new TermQuery(new Term("field", "value")), convert(json.toString()));
    }
    
    @Test
    public void testMatchAllDocsQuery() {
        JsonObject json = Json.createObjectBuilder()
                .add("type", "MatchAllDocsQuery")
                .build();
        assertEquals(new MatchAllDocsQuery(), convert(json.toString()));
    }

    private Query convert(String queryString) {
        return new QueryStringToQuery(new StringReader(queryString)).convert();
    }
}

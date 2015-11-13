package test;

import static org.junit.Assert.assertEquals;

import java.io.StringReader;
import java.util.List;

import javax.json.Json;
import javax.json.JsonObject;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.junit.Test;
import org.meresco.lucene.QueryStringToQuery;
import org.meresco.lucene.QueryStringToQuery.FacetRequest;

public class QueryStringToQueryTest {

    @Test
    public void testTermQuery() {
        JsonObject json = Json.createObjectBuilder()
                .add("query", Json.createObjectBuilder()
                    .add("type", "TermQuery")
                    .add("term", Json.createObjectBuilder()
                        .add("field", "field")
                        .add("value", "value")))
                .build();
        QueryStringToQuery q = new QueryStringToQuery(new StringReader(json.toString()));
        assertEquals(new TermQuery(new Term("field", "value")), q.query);
    }
    
    @Test
    public void testMatchAllDocsQuery() {
        JsonObject json = Json.createObjectBuilder()
                .add("query", Json.createObjectBuilder()
                        .add("type", "MatchAllDocsQuery"))
                .build();
        QueryStringToQuery q = new QueryStringToQuery(new StringReader(json.toString()));
        assertEquals(new MatchAllDocsQuery(), q.query);
    }
    
    @Test
    public void testFacets() {
        JsonObject json = Json.createObjectBuilder()
                .add("query", Json.createObjectBuilder()
                        .add("type", "MatchAllDocsQuery"))
                .add("facets", Json.createArrayBuilder()
                        .add(Json.createObjectBuilder()
                                .add("fieldname", "fieldname")
                                .add("maxTerms", 10)))
                .build();
        QueryStringToQuery q = new QueryStringToQuery(new StringReader(json.toString()));
        assertEquals(new MatchAllDocsQuery(), q.query);
        List<FacetRequest> facets = q.facets;
        assertEquals(1, facets.size());
        assertEquals("fieldname", facets.get(0).fieldname);
        assertEquals(10, facets.get(0).maxTerms);
    }
    
    @Test
    public void testSortKeys() {
        JsonObject json = Json.createObjectBuilder()
                .add("query", Json.createObjectBuilder()
                        .add("type", "MatchAllDocsQuery"))
                .add("sortKeys", Json.createArrayBuilder()
                        .add(Json.createObjectBuilder()
                                .add("sortBy", "fieldname")
                                .add("type", "String")
                                .add("sortDescending", false))
                        .add(Json.createObjectBuilder()
                                .add("sortBy", "score")
                                .add("sortDescending", true))
                        .add(Json.createObjectBuilder()
                                .add("sortBy", "intfield")
                                .add("type", "Int")
                                .add("sortDescending", true))
                        .add(Json.createObjectBuilder()
                                .add("sortBy", "fieldname")
                                .add("type", "String")
                                .add("missingValue", "STRING_FIRST")
                                .add("sortDescending", true)))
                .build();
        QueryStringToQuery q = new QueryStringToQuery(new StringReader(json.toString()));
        assertEquals(new MatchAllDocsQuery(), q.query);
        SortField[] sortFields = q.sort.getSort();
        assertEquals(4, sortFields.length);
    }
}

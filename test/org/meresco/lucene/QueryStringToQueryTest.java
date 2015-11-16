package org.meresco.lucene;

import static org.junit.Assert.assertEquals;

import java.io.StringReader;
import java.util.List;

import javax.json.Json;
import javax.json.JsonObject;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.BooleanClause.Occur;
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
    public void testTermQueryWithBoost() {
        JsonObject json = Json.createObjectBuilder()
                .add("query", Json.createObjectBuilder()
                    .add("type", "TermQuery")
                    .add("boost", 2.0)
                    .add("term", Json.createObjectBuilder()
                        .add("field", "field")
                        .add("value", "value")))
                .build();
        QueryStringToQuery q = new QueryStringToQuery(new StringReader(json.toString()));
        TermQuery query = new TermQuery(new Term("field", "value"));
        query.setBoost(2.0f);
        assertEquals(query, q.query);
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
    public void testBooleanShouldQuery() {
        JsonObject json = Json.createObjectBuilder()
                .add("query", Json.createObjectBuilder()
                    .add("type", "BooleanQuery")
                    .add("clauses", Json.createArrayBuilder()
                            .add(Json.createObjectBuilder()
                                .add("type", "TermQuery")
                                .add("boost", 1.0)
                                .add("occur", "SHOULD")
                                .add("term", Json.createObjectBuilder()
                                    .add("field", "aField")
                                    .add("value", "value")))
                            .add(Json.createObjectBuilder()
                                .add("type", "TermQuery")
                                .add("boost", 2.0)
                                .add("occur", "SHOULD")
                                .add("term", Json.createObjectBuilder()
                                    .add("field", "oField")
                                    .add("value", "value")))))
                .build();
        QueryStringToQuery q = new QueryStringToQuery(new StringReader(json.toString()));
        TermQuery aQuery = new TermQuery(new Term("aField", "value"));
        aQuery.setBoost(1.0f);
        TermQuery oQuery = new TermQuery(new Term("oField", "value"));
        oQuery.setBoost(2.0f);
        BooleanQuery query = new BooleanQuery();
        query.add(aQuery, Occur.SHOULD);
        query.add(oQuery, Occur.SHOULD);
        assertEquals(query, q.query);
    }
   
    @Test
    public void testBooleanMustQuery() {
        JsonObject json = Json.createObjectBuilder()
                .add("query", Json.createObjectBuilder()
                    .add("type", "BooleanQuery")
                    .add("clauses", Json.createArrayBuilder()
                            .add(Json.createObjectBuilder()
                                .add("type", "TermQuery")
                                .add("boost", 1.0)
                                .add("occur", "MUST")
                                .add("term", Json.createObjectBuilder()
                                    .add("field", "aField")
                                    .add("value", "value")))
                            .add(Json.createObjectBuilder()
                                .add("type", "TermQuery")
                                .add("boost", 2.0)
                                .add("occur", "MUST_NOT")
                                .add("term", Json.createObjectBuilder()
                                    .add("field", "oField")
                                    .add("value", "value")))))
                .build();
        QueryStringToQuery q = new QueryStringToQuery(new StringReader(json.toString()));
        TermQuery aQuery = new TermQuery(new Term("aField", "value"));
        aQuery.setBoost(1.0f);
        TermQuery oQuery = new TermQuery(new Term("oField", "value"));
        oQuery.setBoost(2.0f);
        BooleanQuery query = new BooleanQuery();
        query.add(aQuery, Occur.MUST);
        query.add(oQuery, Occur.MUST_NOT);
        assertEquals(query, q.query);
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
        assertEquals("fieldname", sortFields[0].getField());
        assertEquals(SortField.Type.STRING, sortFields[0].getType());
        assertEquals(true, sortFields[0].getReverse());
        assertEquals(null, sortFields[0].missingValue);
        
        assertEquals(null, sortFields[1].getField());
        assertEquals(SortField.Type.SCORE, sortFields[1].getType());
        assertEquals(false, sortFields[1].getReverse());
        assertEquals(null, sortFields[1].missingValue);
        
        assertEquals("intfield", sortFields[2].getField());
        assertEquals(SortField.Type.INT, sortFields[2].getType());
        assertEquals(false, sortFields[2].getReverse());
        assertEquals(null, sortFields[2].missingValue);
        
        assertEquals("fieldname", sortFields[3].getField());
        assertEquals(SortField.Type.STRING, sortFields[3].getType());
        assertEquals(false, sortFields[3].getReverse());
        assertEquals(SortField.STRING_FIRST, sortFields[3].missingValue);
    }
}

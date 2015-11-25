package org.meresco.lucene;

import static org.junit.Assert.*;

import java.io.StringReader;
import java.util.HashSet;

import javax.json.Json;
import javax.json.JsonObject;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermQuery;
import org.junit.Before;
import org.junit.Test;

public class ComposedQueryTest {

    @SuppressWarnings("serial")
    @Test
    public void testComposedQuery() throws Exception {
        JsonObject json = Json.createObjectBuilder()
                .add("resultsFrom", "coreA")
                .add("start", 1)
                .add("stop", 10)
                .add("sortKeys", Json.createArrayBuilder()
                        .add(Json.createObjectBuilder()
                                .add("sortBy", "fieldname")
                                .add("type", "String")
                                .add("sortDescending", false)))
                .add("cores", Json.createArrayBuilder()
                    .add("coreA")
                    .add("coreB"))
                .add("matches", Json.createObjectBuilder()
                    .add("coreA->coreB", Json.createArrayBuilder()
                        .add(Json.createObjectBuilder()
                            .add("core", "coreA")
                            .add("uniqueKey", "keyA"))
                        .add(Json.createObjectBuilder()
                            .add("core", "coreB")
                            .add("key", "keyB"))
                    )
                )
                .add("queries", Json.createObjectBuilder()
                    .add("coreA", Json.createObjectBuilder()
                            .add("type", "TermQuery")
                            .add("term", Json.createObjectBuilder()
                                .add("field", "field")
                                .add("value", "value0")))
                    .add("coreB", Json.createObjectBuilder()
                            .add("type", "TermQuery")
                            .add("term", Json.createObjectBuilder()
                                .add("field", "field")
                                .add("value", "value1")))
                )
                .build();
        ComposedQuery q = ComposedQuery.fromJsonString(new StringReader(json.toString()));
        assertEquals("coreA", q.resultsFrom);
        assertEquals(1, q.start);
        assertEquals(10, q.stop);
        assertEquals(new HashSet<String>() {{add("coreA"); add("coreB");}}, q.cores);
        assertEquals(new TermQuery(new Term("field", "value0")), q.queryFor("coreA"));
        assertEquals(new TermQuery(new Term("field", "value1")), q.queryFor("coreB"));
        assertEquals(1, q.sort.getSort().length);
        
        assertEquals("keyA", q.keyName("coreA", "coreB"));
        assertEquals("keyB", q.keyName("coreB", "coreA"));
        assertEquals("keyA", q.keyName("coreA", "coreA"));
        assertEquals("keyB", q.keyName("coreB", "coreB"));
        
        assertArrayEquals(new String[] {"keyA"}, q.keyNames("coreA"));
    }
    
    @Test
    public void testStartStopNotRequired() throws Exception {
        JsonObject json = Json.createObjectBuilder()
                    .add("resultsFrom", "coreA")
                    .build();
        ComposedQuery q = ComposedQuery.fromJsonString(new StringReader(json.toString()));
        assertEquals(0, q.start);
        assertEquals(10, q.stop);
    }
}

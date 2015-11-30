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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.StringReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import javax.json.Json;
import javax.json.JsonObject;

import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.junit.Test;
import org.meresco.lucene.ComposedQuery.Unite;
import org.meresco.lucene.QueryConverter.FacetRequest;

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
                .add("suggestionRequest", Json.createObjectBuilder()
                        .add("field", "field1")
                        .add("count", 2)
                        .add("suggests", Json.createArrayBuilder()
                            .add("valeu")))
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
                .add("filterQueries", Json.createObjectBuilder()
                    .add("coreA", Json.createArrayBuilder()
                        .add(Json.createObjectBuilder()
                            .add("type", "TermQuery")
                            .add("term", Json.createObjectBuilder()
                                .add("field", "field")
                                .add("value", "value0")))))
                .add("otherCoreFacetFilters", Json.createObjectBuilder()
                    .add("coreA", Json.createArrayBuilder()
                        .add(Json.createObjectBuilder()
                            .add("type", "TermQuery")
                            .add("term", Json.createObjectBuilder()
                                .add("field", "field")
                                .add("value", "value0")))))
                .add("drilldownQueries", Json.createObjectBuilder()
                    .add("coreA", Json.createArrayBuilder()
                        .add(Json.createArrayBuilder()
                            .add("ddField")
                            .add(Json.createArrayBuilder().add("ddValue")))))
                .add("rankQueries", Json.createObjectBuilder()
                    .add("coreA", Json.createObjectBuilder()
                        .add("type", "TermQuery")
                        .add("term", Json.createObjectBuilder()
                            .add("field", "field")
                            .add("value", "value0"))))
                .add("facets", Json.createObjectBuilder()
                    .add("coreA", Json.createArrayBuilder()
                        .add(Json.createObjectBuilder()
                            .add("fieldname", "fieldA")
                            .add("maxTerms", 10)))
                    .add("coreB", Json.createArrayBuilder()
                        .add(Json.createObjectBuilder()
                            .add("fieldname", "fieldB")
                            .add("maxTerms", 5))))
                .build();
        Map<String, QueryConverter> queryConverters = new HashMap<String, QueryConverter>() {{
            put("coreA", new QueryConverter(new FacetsConfig()));
            put("coreB", new QueryConverter(new FacetsConfig()));
        }};
        ComposedQuery q = ComposedQuery.fromJsonString(new StringReader(json.toString()), queryConverters);
        assertEquals("coreA", q.resultsFrom);
        assertEquals(1, q.queryData.start);
        assertEquals(10, q.queryData.stop);
        assertEquals(new HashSet<String>() {{add("coreA"); add("coreB");}}, q.cores);
        assertEquals(new TermQuery(new Term("field", "value0")), q.queryFor("coreA"));
        assertEquals(new TermQuery(new Term("field", "value1")), q.queryFor("coreB"));
        assertEquals(1, q.queryData.sort.getSort().length);

        assertEquals("keyA", q.keyName("coreA", "coreB"));
        assertEquals("keyB", q.keyName("coreB", "coreA"));
        assertEquals("keyA", q.keyName("coreA", "coreA"));
        assertEquals("keyB", q.keyName("coreB", "coreB"));

        assertArrayEquals(new String[] {"keyA"}, q.keyNames("coreA"));

        List<Query> filterQueriesA = q.filterQueries.get("coreA");
        assertEquals(1, filterQueriesA.size());
        assertEquals(new TermQuery(new Term("field", "value0")), filterQueriesA.get(0));
        List<Query> filterQueriesB = q.filterQueries.get("coreB");
        assertEquals(null, filterQueriesB);

        List<FacetRequest> facetsA = q.facetsFor("coreA");
        assertEquals(1, facetsA.size());
        assertEquals("fieldA", facetsA.get(0).fieldname);
        assertEquals(10, facetsA.get(0).maxTerms);

        List<FacetRequest> facetsB = q.facetsFor("coreB");
        assertEquals(1, facetsB.size());
        assertEquals("fieldB", facetsB.get(0).fieldname);
        assertEquals(5, facetsB.get(0).maxTerms);
        
        List<Query> otherCoreFacetFilters = q.otherCoreFacetFiltersFor("coreA");
        assertEquals(1, otherCoreFacetFilters.size());
        assertEquals(new TermQuery(new Term("field", "value0")), otherCoreFacetFilters.get(0));
        
        Map<String, String[]> drilldownQueries = q.drilldownQueriesFor("coreA");
        assertEquals(1, drilldownQueries.size());
        assertArrayEquals(new String[] {"ddValue"}, drilldownQueries.get("ddField"));
        
        Query rankQuery = q.rankQueryFor("coreA");
        assertEquals(new TermQuery(new Term("field", "value0")), rankQuery);
        
        assertEquals("field1", q.queryData.suggestionRequest.field);
        assertEquals(2, q.queryData.suggestionRequest.count);
        assertArrayEquals(new String[] {"valeu"}, q.queryData.suggestionRequest.suggests.toArray(new String[0]));
    }

    @Test
    public void testStartStopNotRequired() throws Exception {
        JsonObject json = Json.createObjectBuilder()
                    .add("resultsFrom", "coreA")
                    .build();
        Map<String, QueryConverter> queryConverters = new HashMap<String, QueryConverter>() {{
            put("coreA", new QueryConverter(new FacetsConfig()));
        }};
        ComposedQuery q = ComposedQuery.fromJsonString(new StringReader(json.toString()), queryConverters);
        assertEquals(0, q.queryData.start);
        assertEquals(10, q.queryData.stop);
    }
    
    @SuppressWarnings("serial")
    @Test
    public void testUnite() {
        JsonObject json = Json.createObjectBuilder()
                .add("resultsFrom", "coreA")
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
                .add("unites", Json.createArrayBuilder()
                    .add(Json.createObjectBuilder()
                        .add("A", Json.createArrayBuilder()
                            .add("summary")
                            .add(Json.createObjectBuilder()
                                .add("type", "TermQuery")
                                .add("term", Json.createObjectBuilder()
                                    .add("field", "field")
                                    .add("value", "value0"))))
                        .add("B", Json.createArrayBuilder()
                            .add("holding")
                            .add(Json.createObjectBuilder()
                                .add("type", "TermQuery")
                                .add("term", Json.createObjectBuilder()
                                    .add("field", "field2")
                                    .add("value", "value1")))) 
                    ))
                .build();
        Map<String, QueryConverter> queryConverters = new HashMap<String, QueryConverter>() {{
            put("summary", new QueryConverter(new FacetsConfig()));
            put("holding", new QueryConverter(new FacetsConfig()));
        }};
        ComposedQuery q = ComposedQuery.fromJsonString(new StringReader(json.toString()), queryConverters);
        List<Unite> unites = q.getUnites();
        assertEquals(1, unites.size());
        Unite unite = unites.get(0);
        assertEquals("summary", unite.coreA);
        assertEquals("holding", unite.coreB);
        assertEquals(new TermQuery(new Term("field", "value0")), unite.queryA);
        assertEquals(new TermQuery(new Term("field2", "value1")), unite.queryB);
    }
}

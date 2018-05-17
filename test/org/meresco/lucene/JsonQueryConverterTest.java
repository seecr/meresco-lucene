/* begin license *
 *
 * "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
 *
 * Copyright (C) 2015-2016 Koninklijke Bibliotheek (KB) http://www.kb.nl
 * Copyright (C) 2015-2016 Seecr (Seek You Too B.V.) http://seecr.nl
 * Copyright (C) 2016 Stichting Kennisnet http://www.kennisnet.nl
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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.StringReader;
import java.util.List;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonValue;

import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.facet.DrillDownQuery;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.search.spell.SuggestMode;
import org.junit.Test;
import org.meresco.lucene.JsonQueryConverter.FacetRequest;
import org.meresco.lucene.search.JoinSortField;
import org.meresco.lucene.search.join.relational.JoinAndQuery;
import org.meresco.lucene.search.join.relational.JoinOrQuery;
import org.meresco.lucene.search.join.relational.RelationalLuceneQuery;
import org.meresco.lucene.search.join.relational.RelationalNotQuery;
import org.meresco.lucene.search.join.relational.WrappedRelationalQuery;


public class JsonQueryConverterTest {
    private JsonQueryConverter queryConverter = new JsonQueryConverter(new FacetsConfig(), "coreA");

    @Test
    public void testTermQuery() {
        JsonObject json = Json.createObjectBuilder()
                .add("query", Json.createObjectBuilder()
                    .add("type", "TermQuery")
                    .add("term", Json.createObjectBuilder()
                        .add("field", "field")
                        .add("value", "value")))
                .build();
        QueryData q = new QueryData(new StringReader(json.toString()), queryConverter);
        assertEquals(new TermQuery(new Term("field", "value")), q.query);
    }

    @Test
    public void testTermQueryWithBoost() {
        JsonObject json = Json.createObjectBuilder()
                .add("query", Json.createObjectBuilder()
                    .add("type", "TermQuery")
                    .add("boost", 2.1)
                    .add("term", Json.createObjectBuilder()
                        .add("field", "field")
                        .add("value", "value")))
                .build();
        QueryData q = new QueryData(new StringReader(json.toString()), queryConverter);
        Query query = new TermQuery(new Term("field", "value"));
        query = new BoostQuery(query, 2.1f);
        assertEquals(query, q.query);
    }

    @Test
    public void testMatchAllDocsQuery() {
        JsonObject json = Json.createObjectBuilder()
                .add("query", Json.createObjectBuilder()
                        .add("type", "MatchAllDocsQuery"))
                .build();
        QueryData q = new QueryData(new StringReader(json.toString()), queryConverter);
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
        QueryData q = new QueryData(new StringReader(json.toString()), queryConverter);
        TermQuery aQuery = new TermQuery(new Term("aField", "value"));
        TermQuery oQuery = new TermQuery(new Term("oField", "value"));
        BooleanQuery.Builder query = new BooleanQuery.Builder();
        query.add(new BoostQuery(aQuery, 1.0f), Occur.SHOULD);
        query.add(new BoostQuery(oQuery, 2.0f), Occur.SHOULD);
        assertEquals(query.build(), q.query);
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
        QueryData q = new QueryData(new StringReader(json.toString()), queryConverter);
        TermQuery aQuery = new TermQuery(new Term("aField", "value"));
        TermQuery oQuery = new TermQuery(new Term("oField", "value"));
        BooleanQuery.Builder query = new BooleanQuery.Builder();
        query.add(new BoostQuery(aQuery, 1.0f), Occur.MUST);
        query.add(new BoostQuery(oQuery, 2.0f), Occur.MUST_NOT);
        assertEquals(query.build(), q.query);
    }

    @Test
    public void testWildcardQuery() {
        JsonObject json = Json.createObjectBuilder()
                .add("query", Json.createObjectBuilder()
                    .add("type", "WildcardQuery")
                    .add("term", Json.createObjectBuilder()
                        .add("field", "field")
                        .add("value", "???*")))
                .build();
        QueryData q = new QueryData(new StringReader(json.toString()), queryConverter);
        WildcardQuery query = new WildcardQuery(new Term("field", "???*"));
        assertEquals(query, q.query);
    }

    @Test
    public void testPrefixQuery() {
        JsonObject json = Json.createObjectBuilder()
                .add("query", Json.createObjectBuilder()
                    .add("type", "PrefixQuery")
                    .add("term", Json.createObjectBuilder()
                        .add("field", "field")
                        .add("value", "fiet")))
                .build();
        QueryData q = new QueryData(new StringReader(json.toString()), queryConverter);
        PrefixQuery query = new PrefixQuery(new Term("field", "fiet"));
        assertEquals(query, q.query);
    }

    @Test
    public void testPhraseQuery() {
        JsonObject json = Json.createObjectBuilder()
                .add("query", Json.createObjectBuilder()
                    .add("type", "PhraseQuery")
                    .add("terms", Json.createArrayBuilder()
                        .add(Json.createObjectBuilder()
                            .add("field", "field")
                            .add("value", "phrase"))
                        .add(Json.createObjectBuilder()
                            .add("field", "field")
                            .add("value", "query"))))
                .build();
        QueryData q = new QueryData(new StringReader(json.toString()), queryConverter);
        PhraseQuery.Builder query = new PhraseQuery.Builder();
        query.add(new Term("field", "phrase"));
        query.add(new Term("field", "query"));
        assertEquals(query.build(), q.query);
    }

    @Test
    public void testTermRangeQueryBigger() {
        JsonObject json = Json.createObjectBuilder()
                .add("query", Json.createObjectBuilder()
                    .add("type", "RangeQuery")
                    .add("rangeType", "String")
                    .add("field", "field")
                    .add("lowerTerm", "value")
                    .add("upperTerm", JsonValue.NULL)
                    .add("includeLower", JsonValue.FALSE)
                    .add("includeUpper", JsonValue.FALSE))
                .build();
        QueryData q = new QueryData(new StringReader(json.toString()), queryConverter);
        TermRangeQuery query = TermRangeQuery.newStringRange("field", "value", null, false, false);
        assertEquals(query, q.query);
    }

    @Test
    public void testTermRangeQueryLower() {
        JsonObject json = Json.createObjectBuilder()
                .add("query", Json.createObjectBuilder()
                    .add("type", "RangeQuery")
                    .add("rangeType", "String")
                    .add("field", "field")
                    .add("lowerTerm", JsonValue.NULL)
                    .add("upperTerm", "value")
                    .add("includeLower", JsonValue.TRUE)
                    .add("includeUpper", JsonValue.TRUE))
                .build();
        QueryData q = new QueryData(new StringReader(json.toString()), queryConverter);
        TermRangeQuery query = TermRangeQuery.newStringRange("field", null, "value", true, true);
        assertEquals(query, q.query);
    }

    @Test
    public void testIntRangeQuery() {
        JsonObject json = Json.createObjectBuilder()
                .add("query", Json.createObjectBuilder()
                    .add("type", "RangeQuery")
                    .add("rangeType", "Int")
                    .add("field", "field")
                    .add("lowerTerm", 1)
                    .add("upperTerm", 5)
                    .add("includeLower", JsonValue.FALSE)
                    .add("includeUpper", JsonValue.TRUE))
                .build();
        QueryData q = new QueryData(new StringReader(json.toString()), queryConverter);
        Query query = IntPoint.newRangeQuery("field", 2, 5);
        assertEquals(query, q.query);
    }

    @Test
    public void testIntRangeQueryWithNoBounds() {
        JsonObject json = Json.createObjectBuilder()
                .add("query", Json.createObjectBuilder()
                    .add("type", "RangeQuery")
                    .add("rangeType", "Int")
                    .add("field", "field")
                    .add("lowerTerm", JsonValue.NULL)
                    .add("upperTerm", JsonValue.NULL)
                    .add("includeLower", JsonValue.FALSE)
                    .add("includeUpper", JsonValue.TRUE))
                .build();
        QueryData q = new QueryData(new StringReader(json.toString()), queryConverter);
        Query query = IntPoint.newRangeQuery("field", Integer.MIN_VALUE + 1, Integer.MAX_VALUE);
        assertEquals(query, q.query);
    }
    @Test
    public void testLongRangeQuery() {
        JsonObject json = Json.createObjectBuilder()
                .add("query", Json.createObjectBuilder()
                    .add("type", "RangeQuery")
                    .add("rangeType", "Long")
                    .add("field", "field")
                    .add("lowerTerm", 1L)
                    .add("upperTerm", 5L)
                    .add("includeLower", JsonValue.FALSE)
                    .add("includeUpper", JsonValue.TRUE))
                .build();
        QueryData q = new QueryData(new StringReader(json.toString()), queryConverter);
        Query query = LongPoint.newRangeQuery("field", 2L, 5L);
        assertEquals(query, q.query);
    }

    @Test
    public void testLongRangeQueryWithNoBounds() {
        JsonObject json = Json.createObjectBuilder()
                .add("query", Json.createObjectBuilder()
                    .add("type", "RangeQuery")
                    .add("rangeType", "Long")
                    .add("field", "field")
                    .add("lowerTerm", JsonValue.NULL)
                    .add("upperTerm", JsonValue.NULL)
                    .add("includeLower", JsonValue.FALSE)
                    .add("includeUpper", JsonValue.TRUE))
                .build();
        QueryData q = new QueryData(new StringReader(json.toString()), queryConverter);
        Query query = LongPoint.newRangeQuery("field", Long.MIN_VALUE + 1, Long.MAX_VALUE);
        assertEquals(query, q.query);
    }

    @Test
    public void testDoubleRangeQuery() {
        JsonObject json = Json.createObjectBuilder()
                .add("query", Json.createObjectBuilder()
                    .add("type", "RangeQuery")
                    .add("rangeType", "Double")
                    .add("field", "field")
                    .add("lowerTerm", 1.0)
                    .add("upperTerm", 5.0)
                    .add("includeLower", JsonValue.FALSE)
                    .add("includeUpper", JsonValue.TRUE))
                .build();
        QueryData q = new QueryData(new StringReader(json.toString()), queryConverter);
        Query query = DoublePoint.newRangeQuery("field", Math.nextUp(1.0), 5.0);
        assertEquals(query, q.query);
    }

    @Test
    public void testDoubleRangeQueryWithNoBounds() {
        JsonObject json = Json.createObjectBuilder()
                .add("query", Json.createObjectBuilder()
                    .add("type", "RangeQuery")
                    .add("rangeType", "Double")
                    .add("field", "field")
                    .add("lowerTerm", JsonValue.NULL)
                    .add("upperTerm", JsonValue.NULL)
                    .add("includeLower", JsonValue.FALSE)
                    .add("includeUpper", JsonValue.TRUE))
                .build();
        QueryData q = new QueryData(new StringReader(json.toString()), queryConverter);
        Query query = DoublePoint.newRangeQuery("field", Math.nextUp(Double.NEGATIVE_INFINITY), Double.POSITIVE_INFINITY);
        assertEquals(query, q.query);
    }

    @Test
    public void testDrilldownQuery() {
        JsonObject json = Json.createObjectBuilder()
                .add("query", Json.createObjectBuilder()
                    .add("type", "TermQuery")
                    .add("term", Json.createObjectBuilder()
                        .add("field", "dd-field")
                        .add("path", Json.createArrayBuilder()
                            .add("value"))
                        .add("type", "DrillDown")))
                .build();
        QueryData q = new QueryData(new StringReader(json.toString()), queryConverter);
        TermQuery query = new TermQuery(DrillDownQuery.term("$facets", "dd-field", "value"));
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
                                .add("path", Json.createArrayBuilder()
                                    .add("value1")
                                    .add("subvalue2"))
                                .add("maxTerms", 10)))
                .build();
        QueryData q = new QueryData(new StringReader(json.toString()), queryConverter);
        assertEquals(new MatchAllDocsQuery(), q.query);
        List<FacetRequest> facets = q.facets;
        assertEquals(1, facets.size());
        assertEquals("fieldname", facets.get(0).fieldname);
        assertEquals(10, facets.get(0).maxTerms);
        assertArrayEquals(new String[] {"value1", "subvalue2"}, facets.get(0).path);
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
                                .add("sortDescending", true))
                        .add(Json.createObjectBuilder()
                                .add("sortBy", "fieldname")
                                .add("type", "String")
                                .add("missingValue", "STRING_LAST")
                                .add("sortDescending", true))
                        .add(Json.createObjectBuilder()
                                .add("sortBy", "longfield")
                                .add("type", "Long")
                                .add("sortDescending", true))
                        .add(Json.createObjectBuilder()
                                .add("sortBy", "doublefield")
                                .add("type", "Double")
                                .add("sortDescending", true))
                        .add(Json.createObjectBuilder()
                                .add("sortBy", "stringfield")
                                .add("type", "String")
                                .add("core", "coreA")
                                .add("sortDescending", true))
                        .add(Json.createObjectBuilder()
                                .add("sortBy", "stringfield")
                                .add("type", "String")
                                .add("core", "coreB")
                                .add("sortDescending", true)))
                .build();
        QueryData q = new QueryData(new StringReader(json.toString()), queryConverter);
        assertEquals(new MatchAllDocsQuery(), q.query);
        SortField[] sortFields = q.sort.getSort();
        assertEquals(9, sortFields.length);
        assertEquals("fieldname", sortFields[0].getField());
        assertEquals(SortField.Type.STRING, sortFields[0].getType());
        assertEquals(false, sortFields[0].getReverse());
        assertEquals(null, sortFields[0].getMissingValue());

        assertEquals(null, sortFields[1].getField());
        assertEquals(SortField.Type.SCORE, sortFields[1].getType());
        assertEquals(false, sortFields[1].getReverse());
        assertEquals(null, sortFields[1].getMissingValue());

        assertEquals("intfield", sortFields[2].getField());
        assertEquals(SortField.Type.INT, sortFields[2].getType());
        assertEquals(true, sortFields[2].getReverse());
        assertEquals(null, sortFields[2].getMissingValue());

        assertEquals("fieldname", sortFields[3].getField());
        assertEquals(SortField.Type.STRING, sortFields[3].getType());
        assertEquals(true, sortFields[3].getReverse());
        assertEquals(SortField.STRING_FIRST, sortFields[3].getMissingValue());

        assertEquals("fieldname", sortFields[4].getField());
        assertEquals(SortField.Type.STRING, sortFields[4].getType());
        assertEquals(true, sortFields[4].getReverse());
        assertEquals(SortField.STRING_LAST, sortFields[4].getMissingValue());

        assertEquals("longfield", sortFields[5].getField());
        assertEquals(SortField.Type.LONG, sortFields[5].getType());
        assertEquals(true, sortFields[5].getReverse());
        assertEquals(null, sortFields[5].getMissingValue());

        assertEquals("doublefield", sortFields[6].getField());
        assertEquals(SortField.Type.DOUBLE, sortFields[6].getType());
        assertEquals(true, sortFields[6].getReverse());
        assertEquals(null, sortFields[6].getMissingValue());

        assertEquals("stringfield", sortFields[7].getField());
        assertEquals(SortField.Type.STRING, sortFields[7].getType());
        assertEquals(true, sortFields[7].getReverse());
        assertEquals(null, sortFields[7].getMissingValue());
        assertFalse(sortFields[7] instanceof JoinSortField);

        assertEquals("stringfield", sortFields[8].getField());
        assertEquals(SortField.Type.STRING, sortFields[8].getType());
        assertEquals(true, sortFields[8].getReverse());
        assertEquals(null, sortFields[8].getMissingValue());
        assertTrue(sortFields[8] instanceof JoinSortField);
    }

    @Test
    public void testSuggestionRequest() {
        JsonObject json = Json.createObjectBuilder()
                .add("query", Json.createObjectBuilder()
                    .add("type", "MatchAllDocsQuery"))
                .add("suggestionRequest", Json.createObjectBuilder()
                    .add("field", "field1")
                    .add("count", 2)
                    .add("suggests", Json.createArrayBuilder()
                        .add("valeu")))
                .build();
        QueryData q = new QueryData(new StringReader(json.toString()), queryConverter);
        assertEquals("field1", q.suggestionRequest.field);
        assertEquals(2, q.suggestionRequest.count);
        assertArrayEquals(new String[] {"valeu"}, q.suggestionRequest.suggests.toArray(new String[0]));
    }

    @Test
    public void testSuggestionRequestWithSuggestMode() {
        JsonObject json = Json.createObjectBuilder()
                .add("query", Json.createObjectBuilder()
                    .add("type", "MatchAllDocsQuery"))
                .add("suggestionRequest", Json.createObjectBuilder()
                    .add("field", "field1")
                    .add("count", 2)
                    .add("mode", "SUGGEST_MORE_POPULAR")
                    .add("suggests", Json.createArrayBuilder()
                        .add("valeu")))
                .build();
        QueryData q = new QueryData(new StringReader(json.toString()), queryConverter);
        assertEquals("field1", q.suggestionRequest.field);
        assertEquals(2, q.suggestionRequest.count);
        assertEquals(SuggestMode.SUGGEST_MORE_POPULAR, q.suggestionRequest.mode);
        assertArrayEquals(new String[] {"valeu"}, q.suggestionRequest.suggests.toArray(new String[0]));
    }

    @Test
    public void testDedup() {
        JsonObject json = Json.createObjectBuilder()
                .add("query", Json.createObjectBuilder()
                    .add("type", "MatchAllDocsQuery"))
                .add("dedupField", "__key__")
                .add("dedupSortField", "__key__.date")
                .build();
        QueryData q = new QueryData(new StringReader(json.toString()), queryConverter);
        assertEquals("__key__", q.dedupField);
        assertEquals("__key__.date", q.dedupSortField);
    }

    @Test
    public void testRelationalQuery() {
        JsonObject json = Json.createObjectBuilder()
                .add("query", Json.createObjectBuilder()
                    .add("type", "RelationalNotQuery")
                    .add("query", Json.createObjectBuilder()
                        .add("type", "JoinAndQuery")
                        .add("first", Json.createObjectBuilder()
                            .add("type", "RelationalLuceneQuery")
                            .add("core", "coreA")
                            .add("collectKeyName", "__key__.A")
                            .add("filterKeyName",  "__key__.A")
                            .add("query", Json.createObjectBuilder()
                                .add("type", "TermQuery")
                                .add("term", Json.createObjectBuilder()
                                    .add("field", "field")
                                    .add("value", "value"))))
                        .add("second", Json.createObjectBuilder()
                            .add("type", "JoinOrQuery")
                            .add("first", Json.createObjectBuilder()
                                .add("type", "RelationalLuceneQuery")
                                .add("core", "coreB")
                                .add("collectKeyName", "__key__.B")
                                .add("filterKeyName",  "__key__.A")
                                .add("query", Json.createObjectBuilder()
                                    .add("type", "TermQuery")
                                    .add("term", Json.createObjectBuilder()
                                        .add("field", "field0")
                                        .add("value", "value0"))))
                            .add("second", Json.createObjectBuilder()
                                    .add("type", "RelationalLuceneQuery")
                                    .add("core", "coreA")
                                    .add("collectKeyName", "__key__.A")
                                    .add("filterKeyName",  "__key__.B")
                                    .add("query", Json.createObjectBuilder()
                                        .add("type", "TermQuery")
                                        .add("term", Json.createObjectBuilder()
                                            .add("field", "field1")
                                            .add("value", "value1"))))
                            ))).build();
        QueryData q = new QueryData(new StringReader(json.toString()), queryConverter);
        assertEquals(
            new WrappedRelationalQuery(
                new RelationalNotQuery(
                    new JoinAndQuery(
                        new RelationalLuceneQuery("coreA", "__key__.A", "__key__.A", new TermQuery(new Term("field", "value"))),
                        new JoinOrQuery(
                            new RelationalLuceneQuery("coreB", "__key__.B", "__key__.A", new TermQuery(new Term("field0", "value0"))),
                            new RelationalLuceneQuery("coreA", "__key__.A", "__key__.B", new TermQuery(new Term("field1", "value1"))))))),
            q.query);
    }
}

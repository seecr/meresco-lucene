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

import java.util.ArrayList;
import java.util.List;

import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonValue;

import org.apache.lucene.facet.DrillDownQuery;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.WildcardQuery;

public class QueryConverter {

    private FacetsConfig facetsConfig;

    public QueryConverter(FacetsConfig facetsConfig) {
        this.facetsConfig = facetsConfig;
    }

    Sort convertToSort(JsonArray sortKeys) {
        if (sortKeys == null || sortKeys.size() == 0)
            return null;
        SortField[] sortFields = new SortField[sortKeys.size()];
        for (int i = 0; i < sortKeys.size(); i++) {
            JsonObject sortKey = sortKeys.getJsonObject(i);
            String sortBy = sortKey.getString("sortBy");
            boolean sortDescending = sortKey.getBoolean("sortDescending", false);
            SortField field;
            if (sortBy.equals("score"))
                field = new SortField(null, SortField.Type.SCORE, sortDescending);
            else
                field = new SortField(sortBy, typeForSortField(sortKey.getString("type")), sortDescending);
            Object missingValue = missingSortValue(sortKey.getString("missingValue", null));
            if (missingValue != null)
                field.setMissingValue(missingValue);
            sortFields[i] = field;
        }
        return new Sort(sortFields);
    }

    private Object missingSortValue(String missingValue) {
        if (missingValue == null)
            return null;
        if (missingValue.equals("STRING_FIRST"))
            return SortField.STRING_FIRST;
        else if (missingValue.equals("STRING_LAST"))
            return SortField.STRING_LAST;
        return null;
    }

    private SortField.Type typeForSortField(String type) {
        switch (type) {
            case "String":
                return SortField.Type.STRING;
            case "Int":
                return SortField.Type.INT;
            case "Double":
                return SortField.Type.DOUBLE;
            case "Long":
                return SortField.Type.LONG;
        }
        return null;
    }

    List<FacetRequest> convertToFacets(JsonArray facets) {
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

    Query convertToQuery(JsonObject query) {
        if (query == null)
            return null;
        Query q;
        switch(query.getString("type")) {
            case "MatchAllDocsQuery":
                q = new MatchAllDocsQuery();
                break;
            case "TermQuery":
                q = new TermQuery(createTerm(query.getJsonObject("term")));
                break;
            case "BooleanQuery":
                q = createBooleanQuery(query);
                break;
            case "PhraseQuery":
                q = createPhraseQuery(query);
                break;
            case "PrefixQuery":
                q = new PrefixQuery(createTerm(query.getJsonObject("term")));
                break;
            case "WildcardQuery":
                q = new WildcardQuery(createTerm(query.getJsonObject("term")));
                break;
            case "RangeQuery":
                q = createRangeQuery(query);
                break;
            default:
                return null;
        }
        if (query.get("boost") != null)
            q.setBoost(query.getJsonNumber("boost").longValue());
        return q;
    }

    private Query createPhraseQuery(JsonObject query) {
        PhraseQuery q = new PhraseQuery();
        JsonArray terms = query.getJsonArray("terms");
        for (int i = 0; i < terms.size(); i++) {
            q.add(createTerm(terms.getJsonObject(i)));
        }
        return q;
    }

    private Query createBooleanQuery(JsonObject query) {
        BooleanQuery q = new BooleanQuery();
        JsonArray clauses = query.getJsonArray("clauses");
        for (int i = 0; i < clauses.size(); i++) {
            JsonObject termQ = clauses.getJsonObject(i);
            q.add(convertToQuery(termQ), occurForString(termQ.getString("occur")));
        }
        return q;
    }

    private Query createRangeQuery(JsonObject query) {
        String field = query.getString("field");
        boolean includeLower = query.getBoolean("includeLower");
        boolean includeUpper = query.getBoolean("includeUpper");
        boolean lower = query.get("lowerTerm") != JsonValue.NULL;
        boolean upper = query.get("upperTerm") != JsonValue.NULL;
        switch (query.getString("rangeType")) {
            case "String":
                return TermRangeQuery.newStringRange(field, lower ? query.getString("lowerTerm") : null, upper ? query.getString("upperTerm") : null, includeLower, includeUpper);
            case "Int":
                return NumericRangeQuery.newIntRange(field, lower ? query.getInt("lowerTerm") : null, upper ? query.getInt("upperTerm") : null, includeLower, includeUpper);
            case "Long":
                return NumericRangeQuery.newLongRange(field, lower ? query.getJsonNumber("lowerTerm").longValue() : null, upper ? query.getJsonNumber("upperTerm").longValue() : null, includeLower, includeUpper);
            case "Double":
                return NumericRangeQuery.newDoubleRange(field, lower ? query.getJsonNumber("lowerTerm").doubleValue() : null, upper ? query.getJsonNumber("upperTerm").doubleValue() : null, includeLower, includeUpper);
        }
        return null;
    }

    private Occur occurForString(String occur) {
        switch (occur) {
            case "SHOULD":
                return Occur.SHOULD;
            case "MUST":
                return Occur.MUST;
            case "MUST_NOT":
                return Occur.MUST_NOT;
        }
        return null;
    }

    private Term createTerm(JsonObject term) {
        String type = term.getString("type", null);
        if (type != null && type.equals("DrillDown")) {
            JsonArray jsonPath = term.getJsonArray("path");
            String[] path = new String[jsonPath.size()];
            for (int i = 0; i < jsonPath.size(); i++) {
                path[i] = jsonPath.getString(i);
            }
            return createDrilldownTerm(term.getString("field"), path);
        }
        return new Term(term.getString("field"), term.getString("value"));
    }

    public Term createDrilldownTerm(String field, String... path) {
        String indexFieldName = facetsConfig.getDimConfig(field).indexFieldName;
        return DrillDownQuery.term(indexFieldName, field, path);
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

/* begin license *
 *
 * "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
 *
 * Copyright (C) 2015 Koninklijke Bibliotheek (KB) http://www.kb.nl
 * Copyright (C) 2015-2016 Seecr (Seek You Too B.V.) http://seecr.nl
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
import javax.json.JsonNumber;
import javax.json.JsonObject;
import javax.json.JsonString;
import javax.json.JsonValue;
import javax.json.JsonValue.ValueType;

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
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortField.Type;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.WildcardQuery;
import org.meresco.lucene.search.JoinSortField;

public class QueryConverter {

    private static final String SORT_ON_SCORE = "score";

    private FacetsConfig facetsConfig;

    private String coreName;

    public QueryConverter(FacetsConfig facetsConfig, String coreName) {
        this.facetsConfig = facetsConfig;
        this.coreName = coreName;
    }

    Sort convertToSort(JsonArray sortKeys) {
        if (sortKeys == null || sortKeys.size() == 0)
            return null;
        SortField[] sortFields = new SortField[sortKeys.size()];
        for (int i = 0; i < sortKeys.size(); i++) {
            JsonObject sortKey = sortKeys.getJsonObject(i);
            String sortBy = sortKey.getString("sortBy");
            boolean sortDescending = sortKey.getBoolean("sortDescending", false);
            String core = sortKey.getString("core", null);
            SortField field;
            if (sortBy.equals(SORT_ON_SCORE))
                field = new SortField(null, SortField.Type.SCORE, !sortDescending);
            else {
                Type type = typeForSortField(sortKey.getString("type"));
                if (core == null || core.equals(coreName))
                    field = new SortField(sortBy, type, sortDescending);
                else
                    field = new JoinSortField(sortBy, type, sortDescending, core);
                Object missingValue = missingSortValue(sortKey, type);
                if (missingValue != null)
                    field.setMissingValue(missingValue);
            }
            sortFields[i] = field;
        }
        return new Sort(sortFields);
    }

    private Object missingSortValue(JsonObject sortKey, Type type) {
        JsonValue missingValue = sortKey.getOrDefault("missingValue", null);
        if (missingValue == null)
            return null;
        if (missingValue instanceof JsonString) {
            if (((JsonString) missingValue).getString().equals("STRING_FIRST"))
                return SortField.STRING_FIRST;
            else if (((JsonString) missingValue).getString().equals("STRING_LAST"))
                return SortField.STRING_LAST;
        } else {
            switch (type) {
                case INT:
                    return ((JsonNumber) missingValue).intValue();
                case LONG:
                    return ((JsonNumber) missingValue).longValue();
                case DOUBLE:
                    return ((JsonNumber) missingValue).doubleValue();
                default:
                    break;
            }
        }
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
            if (facet.containsKey("path")) {
                JsonArray jsonPath = facet.getJsonArray("path");
                String[] path = new String[jsonPath.size()];
                for (int j=0; j<path.length; j++)
                    path[j] = jsonPath.getString(j);
                fr.path = path;
            }

            facetRequests.add(fr);
        }
        return facetRequests;
    }


    public SuggestionRequest convertToSuggestionRequest(JsonObject suggestionRequest) {
        if (suggestionRequest == null)
            return null;
        SuggestionRequest sugRequest = new SuggestionRequest(suggestionRequest.getString("field"), suggestionRequest.getInt("count"));
        JsonArray suggests = suggestionRequest.getJsonArray("suggests");
        for (int i=0; i < suggests.size(); i++) {
            sugRequest.suggests.add(suggests.getString(i));
        }
        return sugRequest;
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
            q = new BoostQuery(q, (float) query.getJsonNumber("boost").doubleValue());
        return q;
    }

    private Query createPhraseQuery(JsonObject query) {
        PhraseQuery.Builder b = new PhraseQuery.Builder();
        JsonArray terms = query.getJsonArray("terms");
        for (int i = 0; i < terms.size(); i++) {
            b.add(createTerm(terms.getJsonObject(i)));
        }
        return b.build();
    }

    private Query createBooleanQuery(JsonObject query) {
        BooleanQuery.Builder b = new BooleanQuery.Builder();
        JsonArray clauses = query.getJsonArray("clauses");
        for (int i = 0; i < clauses.size(); i++) {
            JsonObject termQ = clauses.getJsonObject(i);
            b.add(convertToQuery(termQ), occurForString(termQ.getString("occur")));
        }
        return b.build();
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
                Integer iLowerValue = lower ? query.getInt("lowerTerm") : Integer.MIN_VALUE;
                Integer iUpperValue = upper ? query.getInt("upperTerm") : Integer.MAX_VALUE;
                if (!includeLower && iLowerValue != null)
                    iLowerValue += 1;
                if (!includeUpper && iUpperValue != null)
                    iUpperValue -= 1;
                return IntPoint.newRangeQuery(field, iLowerValue, iUpperValue);
            case "Long":
                Long lLowerValue = lower ? query.getJsonNumber("lowerTerm").longValue() : Long.MIN_VALUE;
                Long lUpperValue = upper ? query.getJsonNumber("upperTerm").longValue() : Long.MAX_VALUE;
                if (!includeLower && lLowerValue != null)
                    lLowerValue += 1;
                if (!includeUpper && lUpperValue != null)
                    lUpperValue -= 1;
                return LongPoint.newRangeQuery(field, lLowerValue, lUpperValue);
            case "Double":
                Double dLowerValue = lower ? query.getJsonNumber("lowerTerm").doubleValue() : Double.NEGATIVE_INFINITY;
                Double dUpperValue = upper ? query.getJsonNumber("upperTerm").doubleValue() : Double.POSITIVE_INFINITY;
                if (!includeLower && dLowerValue != null)
                    dLowerValue = Math.nextUp(dLowerValue);
                if (!includeUpper && dUpperValue != null)
                    dUpperValue = Math.nextDown(dUpperValue);
                return DoublePoint.newRangeQuery(field, dLowerValue, dUpperValue);
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

    public static class SuggestionRequest {
        public String field;
        public int count;
        List<String> suggests = new ArrayList<>();

        public SuggestionRequest(String field, int count) {
            this.field = field;
            this.count = count;
        }

        public void add(String suggest) {
            suggests.add(suggest);
        }

    }
}

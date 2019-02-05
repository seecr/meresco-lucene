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

import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.meresco.lucene.JsonQueryConverter.FacetRequest;
import org.meresco.lucene.JsonQueryConverter.SuggestionRequest;

import javax.json.*;


public class QueryData {
    public Query query = new MatchAllDocsQuery();
    public List<FacetRequest> facets;
    public List<String> storedFields = new ArrayList<>();
    public int start = 0;
    public int stop = 10;
    public Sort sort;
    public SuggestionRequest suggestionRequest;
    public String dedupField;
    public String dedupSortField[];
    public boolean clustering;
    public ClusterConfig clusterConfig;

    public QueryData(Reader queryReader, JsonQueryConverter converter) {
        JsonObject object = Json.createReader(queryReader).readObject();
        this.query = converter.convertToQuery(object.getJsonObject("query"));
        this.facets = converter.convertToFacets(object.getJsonArray("facets"));
        JsonArray fields = object.getJsonArray("storedFields");
        if (fields != null)
            fields.stream().forEach(s -> storedFields.add(((JsonString) s).getString()));
        this.start = object.getInt("start", 0);
        this.stop = object.getInt("stop", 10);
        this.sort = converter.convertToSort(object.getJsonArray("sortKeys"));
        this.suggestionRequest = converter.convertToSuggestionRequest(object.getJsonObject("suggestionRequest"));
        this.dedupField = object.getString("dedupField", null);
        getDedupSortFieldsFromJson(object, "dedupSortField");
        this.clustering = object.getBoolean("clustering", false);
        this.clusterConfig = ClusterConfig.parseFromJsonObject(object);
    }

    public QueryData() {

    }

    public void getDedupSortFieldsFromJson(JsonObject object, String fieldName) {
        JsonValue v = object.get(fieldName);
        if (v!=null) {
            if (v.getValueType() == JsonValue.ValueType.ARRAY) {
                JsonArray arr = (JsonArray)v;
                dedupSortField = new String[arr.size()];
                for (int i=0; i<arr.size(); i++) {
                    dedupSortField[i] = arr.getString(i);
                }
            }
            else if (v.getValueType() == JsonValue.ValueType.STRING) {
                dedupSortField = new String[1];
                dedupSortField[0] = ((JsonString)v).getString();
            }
        }
        if (dedupSortField==null) {
            dedupSortField = new String[0];
        }
    }
}

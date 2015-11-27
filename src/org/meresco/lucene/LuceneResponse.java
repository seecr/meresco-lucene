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
import java.util.Arrays;
import java.util.List;

import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;

import org.apache.lucene.facet.LabelAndValue;

public class LuceneResponse {
    public int total;
    public ArrayList<Hit> hits = new ArrayList<Hit>();
    public List<DrilldownData> drilldownData = new ArrayList<DrilldownData>();
    public long queryTime = 0;

    public LuceneResponse(int totalHits) {
        total = totalHits;
    }

    public void addHit(String id, float score) {
        hits.add(new Hit(id, score));
    }

    public static class Hit {
        public String id;
        public float score;

        public Hit(String id, float score) {
            this.id = id;
            this.score = score;
        }
    }

    public static class DrilldownData {
        public String fieldname;
        public String[] path = new String[0];
        public List<Term> terms;

        public DrilldownData(String fieldname) {
            this.fieldname = fieldname;
        }
        
        public boolean equals(Object object) {
            if(object instanceof DrilldownData){
                DrilldownData ddObject = (DrilldownData) object;
                return ddObject.fieldname.equals(fieldname) && Arrays.equals(ddObject.path, path) && ddObject.terms.equals(terms);
            } else {
                return false;
            }
        }
        
        public static class Term {
            public final String label;
            public final int count;
            public List<Term> subTerms;
            
            public Term(String label, int count) {
                this.label = label;
                this.count = count;
            }
        
            public boolean equals(Object object) {
                if(object instanceof Term){
                    Term term = (Term) object;
                    return term.label.equals(label) && term.count == count && ((term.subTerms == null && subTerms == null) || term.subTerms.equals(subTerms));
                } else {
                    return false;
                }
            }
        }
    }

    public JsonObject toJson() {
        JsonObjectBuilder jsonBuilder = Json.createObjectBuilder()
                .add("total", total)
                .add("queryTime", queryTime);

        JsonArrayBuilder hitsArray = Json.createArrayBuilder();
        for (Hit hit : hits) {
            hitsArray.add(Json.createObjectBuilder()
                    .add("id", hit.id)
                    .add("score", hit.score));
        }
        jsonBuilder.add("hits", hitsArray);

        if (drilldownData.size() > 0) {
            JsonArrayBuilder ddArray = Json.createArrayBuilder();
            for (DrilldownData dd : drilldownData) {
                ddArray.add(Json.createObjectBuilder()
                        .add("fieldname", dd.fieldname)
                        .add("path", Json.createArrayBuilder())
                        .add("terms", jsonTermList(dd.terms)));
            }
            jsonBuilder.add("drilldownData", ddArray);
        }
        return jsonBuilder.build();
    }

    private JsonArrayBuilder jsonTermList(List<DrilldownData.Term> terms) {
        JsonArrayBuilder termArray = Json.createArrayBuilder();
        for (DrilldownData.Term term : terms) {
            JsonObjectBuilder termDict = Json.createObjectBuilder()
                    .add("term", term.label)
                    .add("count", term.count);
            if (term.subTerms != null)
                termDict.add("subterms", jsonTermList(term.subTerms));
            termArray.add(termDict);
        }
        return termArray;
    }
}
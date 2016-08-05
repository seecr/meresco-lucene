/* begin license *
 *
 * "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
 *
 * Copyright (C) 2015-2016 Koninklijke Bibliotheek (KB) http://www.kb.nl
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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonValue;

import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.spell.SuggestWord;
import org.apache.lucene.util.FixedBitSet;
import org.meresco.lucene.search.MerescoCluster;
import org.meresco.lucene.search.MerescoCluster.DocScore;
import org.meresco.lucene.search.MerescoCluster.TermScore;

public class LuceneResponse {
    public int total;
    public Integer totalWithDuplicates;
    public List<Hit> hits = new ArrayList<>();
    public List<DrilldownData> drilldownData = new ArrayList<>();
    public long queryTime = 0;
    public Map<String,SuggestWord[]> suggestions = new HashMap<>();
    public Map<String, Long> times = new HashMap<>();
    public FixedBitSet keys;

    public LuceneResponse(int totalHits) {
        total = totalHits;
    }

    public void addHit(Hit hit) {
        hits.add(hit);
    }


    public static class Hit implements Comparable<Hit> {
        public String id;
        public float score;
        public List<IndexableField[]> fields = new ArrayList<>();

        public Hit(String id, float score) {
            this.id = id;
            this.score = score;
        }

        @Override
        public int compareTo(Hit o) {
            return id.compareTo(o.id);
        }

        public String toString() {
        	return "Hit(" + id + ", " + score + ")";
        }

        public IndexableField[] getFields(String fieldname) {
            for (IndexableField[] i: fields) {
                if (i[0].name().equals(fieldname))
                    return i;
            }
            return null;
        }
    }


    public static class DedupHit extends Hit {
        public DedupHit(String id, float score) {
            super(id, score);
        }
        public String duplicateField;
        public int duplicateCount;
    }


    public static class GroupingHit extends Hit {
        public List<String> duplicates;
        public String groupingField;
        public GroupingHit(String id, float score) {
            super(id, score);
        }
    }


    public static class ClusterHit extends Hit {
        public MerescoCluster.DocScore[] topDocs = {};
        public MerescoCluster.TermScore[] topTerms = {};
        public ClusterHit(String id, float score) {
            super(id, score);
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
            JsonObjectBuilder hitBuilder = Json.createObjectBuilder()
                    .add("score", hit.score);
            if (hit.id == null)
                hitBuilder.add("id", JsonValue.NULL);
            else
                hitBuilder.add("id", hit.id);

            for (IndexableField[] fields: hit.fields) {
                if (fields.length == 0)
                    continue;
                JsonArrayBuilder fieldsArray = Json.createArrayBuilder();
                for (IndexableField i: fields) {
                    Number n = i.numericValue();
                    if (n == null) {
                        fieldsArray.add(i.stringValue());
                    } else {
                        if (n instanceof Integer)
                            fieldsArray.add(n.intValue());
                        else if (n instanceof Long)
                            fieldsArray.add(n.longValue());
                        if (n instanceof Double)
                            fieldsArray.add(n.doubleValue());
                    }
                }
                hitBuilder.add(fields[0].name(), fieldsArray);
            }

            if (hit instanceof DedupHit) {
                DedupHit dedupHit = (DedupHit) hit;
                hitBuilder.add("duplicateCount", Json.createObjectBuilder()
                    .add(dedupHit.duplicateField, dedupHit.duplicateCount));
            } else if (hit instanceof GroupingHit) {
                GroupingHit groupingHit = (GroupingHit) hit;
                JsonArrayBuilder duplicatesBuilder = Json.createArrayBuilder();
                for (String id : groupingHit.duplicates)
                    duplicatesBuilder.add(Json.createObjectBuilder().add("id", id));
                hitBuilder.add("duplicates", Json.createObjectBuilder()
                    .add(groupingHit.groupingField, duplicatesBuilder));
            } else if (hit instanceof ClusterHit) {
                ClusterHit clusterHit = (ClusterHit) hit;
                JsonArrayBuilder topDocsBuilder = Json.createArrayBuilder();
                for (DocScore docScore : clusterHit.topDocs) {
                    topDocsBuilder.add(Json.createObjectBuilder()
                        .add("id", docScore.identifier)
                        .add("score", docScore.score));
                }
                JsonArrayBuilder topTermsBuilder = Json.createArrayBuilder();
                for (TermScore termScore : clusterHit.topTerms) {
                    topTermsBuilder.add(Json.createObjectBuilder()
                        .add("term", termScore.term)
                        .add("score", termScore.score));
                }
                hitBuilder.add("duplicates", Json.createObjectBuilder()
                        .add("topDocs", topDocsBuilder)
                        .add("topTerms", topTermsBuilder));
            }
            hitsArray.add(hitBuilder);
        }
        jsonBuilder.add("hits", hitsArray);

        if (totalWithDuplicates != null) {
            jsonBuilder.add("totalWithDuplicates", totalWithDuplicates);
        }

        if (drilldownData.size() > 0) {
            JsonArrayBuilder ddArray = Json.createArrayBuilder();
            for (DrilldownData dd : drilldownData) {
                JsonArrayBuilder path = Json.createArrayBuilder();
                for (String p : dd.path)
                    path.add(p);
                ddArray.add(Json.createObjectBuilder()
                        .add("fieldname", dd.fieldname)
                        .add("path", path)
                        .add("terms", jsonTermList(dd.terms)));
            }
            jsonBuilder.add("drilldownData", ddArray);
        }

        if (times.size() > 0) {
            JsonObjectBuilder timesDict = Json.createObjectBuilder();
            for (String name : times.keySet())
                timesDict.add(name, times.get(name));
            jsonBuilder.add("times", timesDict);
        }
        if (suggestions.size() > 0) {
            JsonObjectBuilder suggestionsDict = Json.createObjectBuilder();
            for (String suggest : suggestions.keySet()) {
                JsonArrayBuilder suggestionArray = Json.createArrayBuilder();
                for (SuggestWord suggestion : suggestions.get(suggest)) {
                    suggestionArray.add(suggestion.string);
                }
                suggestionsDict.add(suggest, suggestionArray);
            }
            jsonBuilder.add("suggestions", suggestionsDict);
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
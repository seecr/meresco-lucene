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
        public ArrayList<LabelAndValue> terms = new ArrayList<LabelAndValue>();
        
        public DrilldownData(String fieldname) {
            this.fieldname = fieldname;
        }
        public void addTerm(LabelAndValue term) {
            terms.add(term);
        }
        public boolean equals(Object object) {
            if(object instanceof DrilldownData){
                DrilldownData ddObject = (DrilldownData) object;
                return ddObject.fieldname.equals(fieldname) && Arrays.equals(ddObject.path, path) && ddObject.terms.equals(terms);
            } else {
                return false;
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
                JsonArrayBuilder termArray = Json.createArrayBuilder();
                for (LabelAndValue term : dd.terms) {
                    termArray.add(Json.createObjectBuilder()
                            .add("term", term.label)
                            .add("count", term.value.intValue()));
                }
                ddArray.add(Json.createObjectBuilder()
                        .add("fieldname", dd.fieldname)
                        .add("path", Json.createArrayBuilder())
                        .add("terms", termArray));
            }
            jsonBuilder.add("drilldownData", ddArray);
        }
        return jsonBuilder.build();
    }
}
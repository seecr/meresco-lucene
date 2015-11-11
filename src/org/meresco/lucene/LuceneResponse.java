package org.meresco.lucene;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;

public class LuceneResponse {
    public int total;
    public ArrayList<Hit> hits = new ArrayList<Hit>();
    public List<DrilldownData> drilldownData = null;
    
    public LuceneResponse(int totalHits) {
        total = totalHits;
    }   
    
    public void addHit(String id) {
        hits.add(new Hit(id));
    }
     
    public class Hit {
        public String id;
        
        public Hit(String id) {
            this.id = id;
        }
    }
    
    public class DrilldownData {
        public String fieldname;
        public String[] path;
        public Map<String, Integer> terms = new HashMap<String, Integer>();
        
        public DrilldownData(String fieldname) {
            this.fieldname = fieldname;
        }
        public void addTerm(String label, int value) {
            terms.put(label, value);
        }
    }
    
    public JsonObject toJson() {
        JsonArrayBuilder hitsArray = Json.createArrayBuilder();
        for (Hit hit : hits) {
            hitsArray.add(Json.createObjectBuilder()
                    .add("id", hit.id));
        }
        JsonObjectBuilder jsonBuilder = Json.createObjectBuilder()
                .add("total", total)
                .add("hits", hitsArray);
                
        if (drilldownData != null) {
            JsonArrayBuilder ddArray = Json.createArrayBuilder();
            for (DrilldownData dd : drilldownData) {
                JsonArrayBuilder termArray = Json.createArrayBuilder();
                for (String key : dd.terms.keySet()) {
                    termArray.add(Json.createObjectBuilder()
                            .add("term", key)
                            .add("count", dd.terms.get(key)));
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
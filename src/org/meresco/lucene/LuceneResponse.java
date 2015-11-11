package org.meresco.lucene;

import java.util.ArrayList;

import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;

public class LuceneResponse {
    public int total;
    public ArrayList<Hit> hits = new ArrayList<Hit>();
    
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
    
    public JsonObject toJson() {
        JsonArrayBuilder hitsArray = Json.createArrayBuilder();
        for (Hit hit : hits) {
            hitsArray.add(Json.createObjectBuilder()
                    .add("id", hit.id));
        }
        JsonObject json = Json.createObjectBuilder()
                .add("total", total)
                .add("hits", hitsArray)
                .build();
        return json;
    }
}
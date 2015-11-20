package org.meresco.lucene;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;

import javax.json.JsonArray;
import javax.json.JsonObject;

import org.junit.Test;
import org.meresco.lucene.LuceneResponse.DrilldownData;

public class LuceneResponseToJson {

    @Test
    public void test() {
        LuceneResponse response = new LuceneResponse(2);
        response.addHit("id1", 0.1f);
        response.addHit("id2", 0.2f);
        LuceneResponse.DrilldownData dd = new DrilldownData("field");
        dd.addTerm("value1", 1);
        dd.addTerm("value2", 5);
        response.drilldownData = new ArrayList<LuceneResponse.DrilldownData>();
        response.drilldownData.add(dd);
        JsonObject jsonResponse = response.toJson();
        assertEquals(2, jsonResponse.getInt("total"));
        assertEquals(0, jsonResponse.getInt("queryTime"));
        JsonArray hits = jsonResponse.getJsonArray("hits");
        assertEquals(2, hits.size());
        assertEquals("id1", hits.getJsonObject(0).getString("id"));
        assertEquals(0.1, hits.getJsonObject(0).getJsonNumber("score").doubleValue(), 0.0001);
        assertEquals("id2", hits.getJsonObject(1).getString("id"));
        assertEquals(0.2, hits.getJsonObject(1).getJsonNumber("score").doubleValue(), 0.0001);
        JsonArray ddData = jsonResponse.getJsonArray("drilldownData");
        assertEquals(1, ddData.size());
        assertEquals("field", ddData.getJsonObject(0).getString("fieldname"));
        assertEquals(0, ddData.getJsonObject(0).getJsonArray("path").size());
        assertEquals("value1", ddData.getJsonObject(0).getJsonArray("terms").getJsonObject(0).getString("term"));
        assertEquals(1, ddData.getJsonObject(0).getJsonArray("terms").getJsonObject(0).getInt("count"));
    }

}

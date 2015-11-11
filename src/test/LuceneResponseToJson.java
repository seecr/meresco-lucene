package test;

import static org.junit.Assert.*;

import javax.json.JsonArray;
import javax.json.JsonObject;

import org.junit.Before;
import org.junit.Test;
import org.meresco.lucene.LuceneResponse;

public class LuceneResponseToJson {

    @Before
    public void setUp() throws Exception {
    }

    @Test
    public void test() {
        LuceneResponse response = new LuceneResponse(2);
        response.addHit("id1");
        response.addHit("id2");
        JsonObject jsonResponse = response.toJson();
        assertEquals(2, jsonResponse.getInt("total"));
        JsonArray hits = jsonResponse.getJsonArray("hits");
        assertEquals(2, hits.size());
        assertEquals("id1", hits.getJsonObject(0).getString("id"));
        assertEquals("id2", hits.getJsonObject(1).getString("id"));
    }

}

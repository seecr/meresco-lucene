package org.meresco.lucene.suggestion;

import static org.junit.Assert.assertEquals;

import javax.json.JsonArray;
import javax.json.JsonValue;

import org.junit.Before;
import org.junit.Test;
import org.meresco.lucene.suggestion.SuggestionNGramIndex.Suggestion;

public class SuggestionHandlerTest {

    @Before
    public void setUp() throws Exception {
    }

    @Test
    public void testJsonSuggestions() {
        Suggestion[] suggestions = new Suggestion[2];
        suggestions[0] = new Suggestion("noot", "uri:book", "uri:me", 1.0f);
        suggestions[1] = new Suggestion("noot", null, null, 1.0f);
        
        JsonArray result = SuggestionHandler.suggestionsToJson(suggestions);
        assertEquals(2, result.size());
        assertEquals("noot", result.getJsonObject(0).getString("suggestion"));
        assertEquals("uri:book", result.getJsonObject(0).getString("type"));
        assertEquals("uri:me", result.getJsonObject(0).getString("creator"));
        assertEquals(1.0, result.getJsonObject(0).getJsonNumber("score").doubleValue(), 0);
        
        assertEquals("noot", result.getJsonObject(1).getString("suggestion"));
        assertEquals(JsonValue.NULL, result.getJsonObject(1).get("type"));
        assertEquals(JsonValue.NULL, result.getJsonObject(1).get("creator"));
        assertEquals(1.0, result.getJsonObject(1).getJsonNumber("score").doubleValue(), 0);
    }

    
}

/* begin license *
 *
 * "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
 *
 * Copyright (C) 2016 Koninklijke Bibliotheek (KB) http://www.kb.nl
 * Copyright (C) 2016 Seecr (Seek You Too B.V.) http://seecr.nl
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

package org.meresco.lucene.suggestion;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertArrayEquals;

import javax.json.JsonArray;
import javax.json.JsonValue;
import javax.json.Json;

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

    @Test
    public void testJsonArrayToStringArray() {
        JsonArray result = Json.createArrayBuilder()
            .add("fiets")
            .add("water")
            .add(JsonValue.NULL)
            .build();
        assertArrayEquals(new String[] {"fiets", "water", null}, SuggestionHandler.jsonArrayToStringArray(result));
    }
}

/* begin license *
 *
 * "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
 *
 * Copyright (C) 2016 Koninklijke Bibliotheek (KB) http://www.kb.nl
 * Copyright (C) 2016 Seecr (Seek You Too B.V.) https://seecr.nl
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

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonValue;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.meresco.lucene.OutOfMemoryShutdown;
import org.meresco.lucene.Utils;
import org.meresco.lucene.http.AbstractMerescoLuceneHandler;
import org.meresco.lucene.suggestion.SuggestionIndex.IndexingState;
import org.meresco.lucene.suggestion.SuggestionNGramIndex.Suggestion;


public class SuggestionHandler extends AbstractMerescoLuceneHandler implements Handler {
    private SuggestionIndex suggestionIndex;
    private int DEFAULT_LIMIT = 10;

    public SuggestionHandler(SuggestionIndex suggestionIndex, OutOfMemoryShutdown shutdown) {
        super(shutdown);
        this.suggestionIndex = suggestionIndex;
    }

    public void doHandle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws Exception {
    	switch (request.getRequestURI()) {
        	case "/add":
        	    String identifier = request.getParameter("identifier");
        	    JsonObject add = Json.createReader(request.getReader()).readObject();
        	    int key = add.getInt("key");
        	    suggestionIndex.add(identifier, key, jsonArrayToStringArray(add.getJsonArray("values")), jsonArrayToStringArray(add.getJsonArray("types")), jsonArrayToStringArray(add.getJsonArray("creators")));
            	break;
        	case "/delete":
                suggestionIndex.delete(request.getParameter("identifier"));
	            break;
        	case "/createSuggestionNGramIndex":
                boolean wait = request.getParameter("wait") == null ? false : request.getParameter("wait").equals("True");
    	        suggestionIndex.createSuggestionNGramIndex(wait, true);
                break;
        	case "/suggest":
        	    JsonObject suggestRequest = Json.createReader(request.getReader()).readObject();
        	    String keySetName = suggestRequest.get("keySetName") == JsonValue.NULL ? null : suggestRequest.getString("keySetName");
                int limit = DEFAULT_LIMIT;
                if (suggestRequest.containsKey("limit") && suggestRequest.get("limit") != JsonValue.NULL) {
                    limit = suggestRequest.getInt("limit");
                }
        	    Suggestion[] suggestions = suggestionIndex.getSuggestionsReader().suggest(suggestRequest.getString("value"), suggestRequest.getBoolean("trigram"), jsonArrayToStringArray(suggestRequest.getJsonArray("filters")), keySetName, limit);
                response.setContentType("application/json");
        	    response.getWriter().write(suggestionsToJson(suggestions).toString());
        	    break;
        	case "/commit":
        	    suggestionIndex.commit();
        	    break;
        	case "/totalRecords":
        	    response.getWriter().write(Integer.toString(suggestionIndex.numDocs()));
        	    break;
        	case "/totalSuggestions":
                response.getWriter().write(Integer.toString(suggestionIndex.getSuggestionsReader().numDocs()));
                break;
        	case "/ngramIndexTimestamp":
                response.getWriter().write(Long.toString(suggestionIndex.ngramIndexTimestamp()));
                break;
            case "/indexingState":
        	    IndexingState state = suggestionIndex.indexingState();
        	    String data = "{}";
        	    if (state != null)
        	        data = "{\"started\": " + state.started + ", \"count\": " + state.count + "}";
                response.setContentType("application/json");
        	    response.getWriter().write(data);
        	    break;
        	case "/registerFilterKeySet":
         	    suggestionIndex.registerFilterKeySet(request.getParameter("name"), Utils.readFixedBitSet(request.getInputStream()));
         	    break;
            default:
        	    response.setStatus(HttpServletResponse.SC_NOT_FOUND);
        	    baseRequest.setHandled(true);
        	    return;
    	}
        response.setStatus(HttpServletResponse.SC_OK);
        baseRequest.setHandled(true);
    }

    static String[] jsonArrayToStringArray(JsonArray jsonStrings) {
        String[] values = new String[jsonStrings.size()];
        for (int i = 0; i < values.length; i++) {
            if (jsonStrings.get(i) == JsonValue.NULL) {
                values[i] = null;
            } else {
                values[i] = jsonStrings.getString(i);
            }
        }
        return values;
    }

    static JsonArray suggestionsToJson(Suggestion[] suggestions) {
        JsonArrayBuilder arrayBuilder = Json.createArrayBuilder();
        for (Suggestion sugg : suggestions) {
            JsonObjectBuilder objectBuilder = Json.createObjectBuilder();
            objectBuilder.add("suggestion", sugg.suggestion);
            objectBuilder.add("score", sugg.score);
            if (sugg.type == null)
                objectBuilder.add("type", JsonValue.NULL);
            else
                objectBuilder.add("type", sugg.type);
            if (sugg.creator == null)
                objectBuilder.add("creator", JsonValue.NULL);
            else
                objectBuilder.add("creator", sugg.creator);
            arrayBuilder.add(objectBuilder);
        }
        return arrayBuilder.build();
    }
}

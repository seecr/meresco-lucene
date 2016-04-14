/* begin license *
 *
 * "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
 *
 * Copyright (C) 2016 Seecr (Seek You Too B.V.) http://seecr.nl
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

import java.io.IOException;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonValue;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.meresco.lucene.OutOfMemoryShutdown;
import org.meresco.lucene.http.AbstractMerescoLuceneHandler;
import org.meresco.lucene.suggestion.SuggestionNGramIndex.Reader;
import org.meresco.lucene.suggestion.SuggestionNGramIndex.Suggestion;

public class SuggestionHandler extends AbstractMerescoLuceneHandler implements Handler {
    private SuggestionIndex suggestionIndex;

    public SuggestionHandler(SuggestionIndex suggestionIndex, OutOfMemoryShutdown shutdown) {
        super(shutdown);
        this.suggestionIndex = suggestionIndex;
    }

    public void doHandle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws Exception {
        if (request.getMethod() == "POST") {
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
        	        suggestionIndex.createSuggestionNGramIndex(true, true);
                    break;
	        	case "/suggest":
	        	    JsonObject suggest = Json.createReader(request.getReader()).readObject();
	        	    String keySetName = suggest.get("keySetName") == JsonValue.NULL ? null : suggest.getString("keySetName"); 
	        	    Suggestion[] suggestions = suggestionIndex.getSuggestionsReader().suggest(suggest.getString("value"), suggest.getBoolean("trigram"), jsonArrayToStringArray(suggest.getJsonArray("filters")), keySetName);
	        	    response.getWriter().write(suggestionsToJson(suggestions).toString());
	        	    break;
	        	case "/commit":
	        	    suggestionIndex.commit();
	        	    break;
	        	default:
	        	    response.setStatus(HttpServletResponse.SC_NOT_FOUND);
	        	    baseRequest.setHandled(true);
	        	    return;
        	}
            response.setStatus(HttpServletResponse.SC_OK);
            baseRequest.setHandled(true);
        } else if (request.getMethod() == "GET") {
            if (request.getRequestURI().equals("/info")) {
                response.setStatus(HttpServletResponse.SC_OK);
//                response.setContentType("application/json");
//                response.getWriter().write("{\"total\": " + termNumerator.size() + "}");
                baseRequest.setHandled(true);
            }
        }
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

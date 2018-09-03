/* begin license *
 *
 * "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
 *
 * Copyright (C) 2015-2016 Koninklijke Bibliotheek (KB) http://www.kb.nl
 * Copyright (C) 2015-2016, 2018 Seecr (Seek You Too B.V.) http://seecr.nl
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

package org.meresco.lucene.http;

import javax.json.Json;
import javax.json.JsonObject;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.lucene.search.Query;
import org.eclipse.jetty.server.Request;
import org.meresco.lucene.Lucene;
import org.meresco.lucene.OutOfMemoryShutdown;


public class DeleteHandler extends AbstractMerescoLuceneHandler {
    private Lucene lucene;

    public DeleteHandler(Lucene lucene, OutOfMemoryShutdown shutdown) {
        super(shutdown);
        this.lucene = lucene;
    }

    @Override
    public void doHandle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws Exception {
        String identifier = request.getParameter("identifier");
        if (identifier != null) {
            this.lucene.deleteDocument(identifier);
        }
        else {
            JsonObject json = Json.createReader(request.getReader()).readObject();
            Query query = this.lucene.getQueryConverter().convertToQuery(json.getJsonObject("query"));
            this.lucene.deleteDocuments(query);
        }
        response.setStatus(HttpServletResponse.SC_OK);
    }
}

/* begin license *
 *
 * "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
 *
 * Copyright (C) 2015-2016 Koninklijke Bibliotheek (KB) http://www.kb.nl
 * Copyright (C) 2015-2016 Seecr (Seek You Too B.V.) http://seecr.nl
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

import java.util.List;

import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Request;
import org.meresco.lucene.Lucene;
import org.meresco.lucene.Lucene.TermCount;
import org.meresco.lucene.OutOfMemoryShutdown;

public class PrefixSearchHandler extends AbstractMerescoLuceneHandler {

    private Lucene lucene;

    public PrefixSearchHandler(Lucene lucene, OutOfMemoryShutdown shutdown) {
        super(shutdown);
        this.lucene = lucene;
    }

    @Override
    public void doHandle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws Exception {
        request.setCharacterEncoding("UTF-8");
        response.setCharacterEncoding("UTF-8");
        String fieldname = request.getParameter("fieldname");
        String prefix = request.getParameter("prefix");
        String limitParam = request.getParameter("limit");
        int limit;
        if (limitParam == null)
            limit = 10;
        else
            limit = Integer.parseInt(limitParam);
        List<TermCount> terms = lucene.termsForField(fieldname, prefix, limit);
        JsonArrayBuilder json = Json.createArrayBuilder();
        for (TermCount t : terms) {
            json.add(Json.createArrayBuilder().add(t.term).add(t.count));
        }
        response.getWriter().write(json.build().toString());
        response.setStatus(HttpServletResponse.SC_OK);
        response.setContentType("application/json");
    }
}

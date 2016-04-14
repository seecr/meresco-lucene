/* begin license *
 *
 * "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
 *
 * Copyright (C) 2016 Koninklijke Bibliotheek (KB) http://www.kb.nl
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

package org.meresco.lucene.http;

import java.io.DataOutputStream;
import java.io.OutputStream;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Request;
import org.meresco.lucene.ComposedQuery;
import org.meresco.lucene.LuceneResponse;
import org.meresco.lucene.MultiLucene;
import org.meresco.lucene.OutOfMemoryShutdown;


public class ExportKeysHandler extends AbstractMerescoLuceneHandler {
    private MultiLucene multiLucene;

    public ExportKeysHandler(MultiLucene multiLucene, OutOfMemoryShutdown shutdown) {
        super(shutdown);
        this.multiLucene = multiLucene;
    }

    @Override
    public void doHandle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws Exception {
        LuceneResponse luceneResponse = new LuceneResponse(0);
        String exportKey = request.getParameter("exportKey");
        ComposedQuery q = ComposedQuery.fromJsonString(request.getReader(), this.multiLucene.getQueryConverters());
        luceneResponse = this.multiLucene.executeComposedQuery(q, exportKey);
        if (luceneResponse.keys == null) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            response.getWriter().write("No keys found; please check that exportKey parameter was provided.");
            baseRequest.setHandled(true);
            return;
        }
        response.setStatus(HttpServletResponse.SC_OK);
        response.setContentType("application/octet-stream");
        OutputStream outputStream = response.getOutputStream();
        DataOutputStream dos = new DataOutputStream(outputStream);
        dos.writeInt(luceneResponse.keys.getNumWords());
        long[] bits = luceneResponse.keys.getBits();
        for (int i = 0; i < bits.length; i++) {
            dos.writeLong(bits[i]);
        }
        dos.flush();
    }
}

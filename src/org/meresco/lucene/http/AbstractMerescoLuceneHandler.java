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

package org.meresco.lucene.http;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.meresco.lucene.Lucene.UninitializedException;
import org.meresco.lucene.OutOfMemoryShutdown;
import org.meresco.lucene.Utils;

public abstract class AbstractMerescoLuceneHandler extends AbstractHandler {

    private final OutOfMemoryShutdown shutdown;

    public AbstractMerescoLuceneHandler(OutOfMemoryShutdown shutdown) {
        this.shutdown = shutdown;
    }

    @Override
    public final void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
        request.setCharacterEncoding("UTF-8");
        response.setCharacterEncoding("UTF-8");
        try {
            doHandle(target, baseRequest, request, response);
        } catch (OutOfMemoryError e) {
            this.shutdown.shutdownInOutOfMemorySituation();
        } catch (UninitializedException e) {
            response.setStatus(HttpServletResponse.SC_CONFLICT);
            response.getWriter().write("Lucene cores should be initialized first. Send a request to /'core-name'/settings to initialize.");
            baseRequest.setHandled(true);
            return;
        } catch (Throwable e) {
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            response.getWriter().write(Utils.getStackTrace(e));
            baseRequest.setHandled(true);
            return;
        }
        baseRequest.setHandled(true);
    }

    public abstract void doHandle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws Throwable;
}

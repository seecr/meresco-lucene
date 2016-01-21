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

package org.meresco.lucene.numerate;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.meresco.lucene.Utils;


public class NumerateHandler extends AbstractHandler implements Handler {
    private TermNumerator termNumerator;

    public NumerateHandler(TermNumerator termNumerator) {
        this.termNumerator = termNumerator;
    }

    @Override
    public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response)
            throws IOException, ServletException {
        response.setCharacterEncoding("UTF-8");
        if (request.getMethod() == "POST") {
        	switch (request.getRequestURI()) {
	        	case "/commit":
	            	try {
	            		termNumerator.commit();
		            } catch (Exception e) {
		                response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
		                response.getWriter().write(Utils.getStackTrace(e));
		                baseRequest.setHandled(true);
		                return;
		            }
	            	break;
	            
	        	case "/numerate":
		            String value = Utils.readFully(request.getReader());
		            int number;
		            try {
		                number = termNumerator.numerateTerm(value);
		            } catch (Exception e) {
		                response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
		                response.getWriter().write(Utils.getStackTrace(e));
		                baseRequest.setHandled(true);
		                return;
		            }
		            response.setContentType("text/plain");
		            response.getWriter().write("" + number);
		            break;
        	}
            response.setStatus(HttpServletResponse.SC_OK);
            baseRequest.setHandled(true);
        } else if (request.getMethod() == "GET") {
            if (request.getRequestURI().equals("/info")) {
                response.setStatus(HttpServletResponse.SC_OK);
                response.setContentType("application/json");
                response.getWriter().write("{\"total\": " + termNumerator.size() + "}");
                baseRequest.setHandled(true);
            }
        }
    }
}

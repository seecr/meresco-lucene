package org.meresco.lucene.http;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.meresco.lucene.TermNumerator;
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
            String value = Utils.readFully(request.getReader());
            int number = termNumerator.numerateTerm(value);
            response.setStatus(HttpServletResponse.SC_OK);
            response.setContentType("text/plain");
            response.getWriter().write("" + number);
            baseRequest.setHandled(true);
        }
    }
}

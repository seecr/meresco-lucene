package org.meresco.lucene.http;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.meresco.lucene.TermNumerator;

public class NumerateHandler extends AbstractHandler implements Handler {
    private TermNumerator termNumerator;

    public NumerateHandler(TermNumerator termNumerator) {
        this.termNumerator = termNumerator;
        // TODO Auto-generated constructor stub
    }

    @Override
    public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response)
            throws IOException, ServletException {
        if (request.getMethod() == "POST") {
            String value = request.getReader().readLine();
        }
            
        // TODO Auto-generated method stub

    }
}

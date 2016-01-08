package org.meresco.lucene.http;

import java.io.IOException;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.meresco.lucene.Lucene;
import org.meresco.lucene.TermNumerator;
import org.meresco.lucene.Utils;


public class CommitHandler extends AbstractHandler implements Handler {
    private TermNumerator termNumerator;
    private List<Lucene> lucenes;

    public CommitHandler(TermNumerator termNumerator, List<Lucene> lucenes) {
        this.termNumerator = termNumerator;
        this.lucenes = lucenes;
    }

    @Override
    public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response)
            throws IOException, ServletException {
        try {
            termNumerator.commit();
            for (Lucene lucene : lucenes) {
                lucene.commit();
            }
        } catch (Exception e) {
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            response.getWriter().write(Utils.getStackTrace(e));
            baseRequest.setHandled(true);
            return;
        }
        response.setStatus(HttpServletResponse.SC_OK);
        baseRequest.setHandled(true);
    }
}

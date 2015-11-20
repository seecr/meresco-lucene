package org.meresco.lucene.http;

import java.io.IOException;
import java.util.List;

import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.lucene.document.Document;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.meresco.lucene.DocumentStringToDocument;
import org.meresco.lucene.Lucene;
import org.meresco.lucene.Lucene.TermCount;

public class PrefixSearchHandler extends AbstractHandler {

    private Lucene lucene;

    public PrefixSearchHandler(Lucene lucene) {
        this.lucene = lucene;
    }
    
    @Override
    public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
        String fieldname = request.getParameter("fieldname");
        String prefix = request.getParameter("prefix");
        String limitParam = request.getParameter("limit");
        int limit;
        if (limitParam == null)
            limit = 10;
        else
            limit = Integer.parseInt(limitParam);
        try {
            List<TermCount> terms = lucene.termsForField(fieldname, prefix, limit);
            JsonArrayBuilder json = Json.createArrayBuilder();
            for (TermCount t : terms) {
                json.add(Json.createArrayBuilder().add(t.term).add(t.count));
            }
            response.getWriter().write(json.build().toString());
        } catch (Exception e) {
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            response.getWriter().write(Utils.getStackTrace(e));
            baseRequest.setHandled(true);
            return;
        }
        response.setStatus(HttpServletResponse.SC_OK);
        response.setContentType("application/json");
        baseRequest.setHandled(true);
    }
}

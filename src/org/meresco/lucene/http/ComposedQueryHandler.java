package org.meresco.lucene.http;

import java.io.IOException;

import javax.json.Json;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.meresco.lucene.ComposedQuery;
import org.meresco.lucene.Lucene;
import org.meresco.lucene.LuceneResponse;
import org.meresco.lucene.MultiLucene;
import org.meresco.lucene.QueryStringToQuery;

public class ComposedQueryHandler extends AbstractHandler {
    private MultiLucene multiLucene;

    public ComposedQueryHandler(MultiLucene multiLucene) {
        this.multiLucene = multiLucene;
    }

    @Override
    public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
        LuceneResponse luceneResponse = new LuceneResponse(0);
        try {
            ComposedQuery q = ComposedQuery.fromJsonString(request.getReader());
            luceneResponse = this.multiLucene.executeComposedQuery(q);
        } catch (Exception e) {
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            response.getWriter().write(Utils.getStackTrace(e));
            baseRequest.setHandled(true);
            return;
        }
        response.setStatus(HttpServletResponse.SC_OK);
        response.setContentType("application/json");
        response.getWriter().write(luceneResponse.toJson().toString());
        baseRequest.setHandled(true);
    }
}

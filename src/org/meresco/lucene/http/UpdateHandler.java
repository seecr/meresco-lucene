package org.meresco.lucene.http;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.lucene.document.Document;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.meresco.lucene.DocumentStringToDocument;
import org.meresco.lucene.Lucene;

public class UpdateHandler extends AbstractHandler {

    private Lucene lucene;

    public UpdateHandler(Lucene lucene) {
        this.lucene = lucene;
    }
    
    @Override
    public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
        String identifier = request.getParameter("identifier");
        try {
          Document document = new DocumentStringToDocument(request.getReader()).convert();
          this.lucene.addDocument(identifier, document);
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

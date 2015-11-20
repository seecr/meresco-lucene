package org.meresco.lucene.http;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.meresco.lucene.Lucene;

public class OtherHandler extends AbstractHandler {

    private Lucene lucene;

    public OtherHandler(Lucene lucene) {
        this.lucene = lucene;
    }
    
    @Override
    public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
        try {
            String result = "";
            switch (target) {
                case "/numDocs/":
                    result = String.valueOf(lucene.numDocs());
                    break;
                case "/maxDoc/":
                    result = String.valueOf(lucene.maxDoc());
                    break;
            }
            response.getWriter().write(result);
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

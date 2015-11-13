package org.meresco.lucene.http;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

import org.meresco.lucene.Lucene;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

public class DeleteHandler implements HttpHandler {

    private Lucene lucene;

    public DeleteHandler(Lucene lucene) {
        this.lucene = lucene;
    }
    
    @Override
    public void handle(HttpExchange exchange) throws IOException {
        OutputStream outputStream = exchange.getResponseBody();
        URI requestURI = exchange.getRequestURI();
        QueryParameters httpArguments = Utils.parseQS(requestURI.getRawQuery());
        
        try {
            this.lucene.deleteDocument(httpArguments.singleValue("identifier"));
        } catch (Exception e) {
            exchange.sendResponseHeaders(500, 0);
            Utils.writeToStream(Utils.getStackTrace(e), outputStream);
            return;
        }
        exchange.sendResponseHeaders(200, 0);
        exchange.close();
    }

}

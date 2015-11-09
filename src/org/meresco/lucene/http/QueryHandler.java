package org.meresco.lucene.http;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

import org.meresco.lucene.Lucene;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

public class QueryHandler implements HttpHandler {

    private Lucene lucene;

    public QueryHandler(Lucene lucene) {
        this.lucene = lucene;
    }
    
    @Override
    public void handle(HttpExchange exchange) throws IOException {
//        OutputStream outputStream = exchange.getResponseBody();
        URI requestURI = exchange.getRequestURI();
//        String path = requestURI.getPath();
        String rawQueryString = requestURI.getRawQuery();
        QueryParameters httpArguments = Utils.parseQS(rawQueryString);
        
        exchange.sendResponseHeaders(200, 0);
        
        exchange.close();
    }

}

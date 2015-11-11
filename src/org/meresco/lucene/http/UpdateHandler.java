package org.meresco.lucene.http;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.net.URI;

import org.apache.lucene.document.Document;
import org.meresco.lucene.DocumentStringToDocument;
import org.meresco.lucene.Lucene;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

public class UpdateHandler implements HttpHandler {

    private Lucene lucene;

    public UpdateHandler(Lucene lucene) {
        this.lucene = lucene;
    }
    
    @Override
    public void handle(HttpExchange exchange) throws IOException {
        OutputStream outputStream = exchange.getResponseBody();
        Reader reader = new InputStreamReader(exchange.getRequestBody());
        URI requestURI = exchange.getRequestURI();
        QueryParameters httpArguments = Utils.parseQS(requestURI.getRawQuery());
        
        try {
            Document document = new DocumentStringToDocument(reader).convert();
            this.lucene.addDocument(httpArguments.singleValue("identifier"), document);
        } catch (Exception e) {
            exchange.sendResponseHeaders(500, 0);
            Utils.writeToStream(Utils.getStackTrace(e), outputStream);
            return;
        }
        exchange.sendResponseHeaders(200, 0);
        exchange.close();
    }

}

package org.meresco.lucene.http;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;

import org.apache.lucene.search.Query;
import org.meresco.lucene.Lucene;
import org.meresco.lucene.LuceneResponse;
import org.meresco.lucene.QueryStringToQuery;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

public class QueryHandler implements HttpHandler {

    private Lucene lucene;

    public QueryHandler(Lucene lucene) {
        this.lucene = lucene;
    }
    
    @Override
    public void handle(HttpExchange exchange) throws IOException {
        OutputStream outputStream = exchange.getResponseBody();
        Reader reader = new InputStreamReader(exchange.getRequestBody());
        
        LuceneResponse response = new LuceneResponse(0);
        try {
            Query query = new QueryStringToQuery(reader).convert();
            response = this.lucene.executeQuery(query, 0, 10, null);
        } catch (Exception e) {
            exchange.sendResponseHeaders(500, 0);
            writeResponse(Utils.getStackTrace(e), outputStream);
            return;
        }
        Headers responseHeaders = exchange.getResponseHeaders();
        responseHeaders.set("Content-Type","application/json");
        exchange.sendResponseHeaders(200, 0);
        writeResponse(response.toJson().toString(), outputStream);
        exchange.close();
    }

    private void writeResponse(String response, OutputStream stream) {
        try {
            Writer writer = new BufferedWriter(new OutputStreamWriter(stream, "UTF-8"));
            writer.write(response, 0, response.length());
            writer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

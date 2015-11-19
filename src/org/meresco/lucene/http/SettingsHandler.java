package org.meresco.lucene.http;

import java.io.IOException;
import java.io.Reader;

import javax.json.Json;
import javax.json.JsonObject;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.meresco.lucene.LuceneSettings;

public class SettingsHandler extends AbstractHandler {

    private LuceneSettings settings;

    public SettingsHandler(LuceneSettings settings) {
        this.settings = settings;
    }

    @Override
    public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
        try {
            updateSettings(this.settings, request.getReader());
        } catch (Exception e) {
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            response.getWriter().write(Utils.getStackTrace(e));
            baseRequest.setHandled(true);
        }
        response.setStatus(HttpServletResponse.SC_OK);
        baseRequest.setHandled(true);
    }

    public static void updateSettings(LuceneSettings settings, Reader reader) {
        JsonObject object = (JsonObject) Json.createReader(reader).read();
        for (String key : object.keySet()) {
            switch (key) {
                case "commitCount":
                    settings.commitCount = object.getInt(key);
                    break;
                case "commitTimeout":
                    settings.commitTimeout = object.getInt(key);
                    break;
            }
        }
    }
}

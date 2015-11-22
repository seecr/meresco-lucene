package org.meresco.lucene.http;

import java.io.IOException;
import java.io.Reader;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonNumber;
import javax.json.JsonObject;
import javax.json.JsonValue;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.Similarity;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.meresco.lucene.Lucene;
import org.meresco.lucene.LuceneSettings;
import org.meresco.lucene.analysis.MerescoDutchStemmingAnalyzer;
import org.meresco.lucene.analysis.MerescoStandardAnalyzer;

public class SettingsHandler extends AbstractHandler {

    private Lucene lucene;

    public SettingsHandler(Lucene lucene) {
        this.lucene = lucene;
    }

    @Override
    public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
        try {
            LuceneSettings settings = lucene.getSettings();
            if (settings == null)
                settings = new LuceneSettings();
            updateSettings(settings, request.getReader());
            if (lucene.getSettings() == null)
                lucene.initSettings(settings);
        } catch (Exception e) {
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            response.getWriter().write(Utils.getStackTrace(e));
            baseRequest.setHandled(true);
        }
        response.setStatus(HttpServletResponse.SC_OK);
        baseRequest.setHandled(true);
    }

    public static void updateSettings(LuceneSettings settings, Reader reader) throws Exception {
        JsonObject object = (JsonObject) Json.createReader(reader).read();
        for (String key : object.keySet()) {
            switch (key) {
                case "commitCount":
                    settings.commitCount = object.getInt(key);
                    break;
                case "commitTimeout":
                    settings.commitTimeout = object.getInt(key);
                    break;
                case "lruTaxonomyWriterCacheSize":
                    settings.lruTaxonomyWriterCacheSize = object.getInt(key);
                    break;
                case "maxMergeAtOnce":
                    settings.maxMergeAtOnce = object.getInt(key);
                    break;
                case "segmentsPerTier":
                    settings.segmentsPerTier = object.getJsonNumber(key).doubleValue();
                    break;
                case "numberOfConcurrentTasks":
                    settings.numberOfConcurrentTasks = object.getInt(key);
                    break;
                case "analyzer":
                    settings.analyzer = getAnalyzer(object.getJsonObject(key));
                    break;
                case "similarity":
                    settings.similarity = getSimilarity(object.getJsonObject(key));
                    break;
                case "drilldownFields":
                    updateDrilldownFields(settings.facetsConfig, object.getJsonArray(key));
            }
        }
        
    }

    private static void updateDrilldownFields(FacetsConfig facetsConfig, JsonArray drilldownFields) {
        for (int i = 0; i < drilldownFields.size(); i++) {
            JsonObject drilldownField = drilldownFields.getJsonObject(i);
            String dim = drilldownField.getString("dim");
            if (drilldownField.get("hierarchical") != null)
                facetsConfig.setHierarchical(dim, drilldownField.getBoolean("hierarchical"));
            if (drilldownField.get("multiValued") != null)
                facetsConfig.setMultiValued(dim, drilldownField.getBoolean("multiValued"));
            JsonValue fieldname = drilldownField.get("fieldname");
            if (fieldname != null)
                facetsConfig.setIndexFieldName(dim, fieldname.toString());
        }   
    }

    private static Similarity getSimilarity(JsonObject similarity) {
        switch (similarity.getString("type")) {
            case "BM25Similarity":
                JsonNumber k1 = similarity.getJsonNumber("k1");
                JsonNumber b = similarity.getJsonNumber("b");
                if (k1 != null && b != null)
                    return new BM25Similarity((float) k1.doubleValue(), (float) b.doubleValue());
                return new BM25Similarity();
        }
        return null;
    }

    private static Analyzer getAnalyzer(JsonObject analyzer) {
        switch (analyzer.getString("type")) {
            case "MerescoDutchStemmingAnalyzer":
                JsonArray jsonFields = analyzer.getJsonArray("fields");
                String[] fields = new String[jsonFields.size()];
                for (int i = 0; i < jsonFields.size(); i++) {
                    fields[i] = jsonFields.getString(i);
                }
                return new MerescoDutchStemmingAnalyzer(fields);
            case "MerescoStandardAnalyzer":
                return new MerescoStandardAnalyzer();
        }
        return null;
    }
}

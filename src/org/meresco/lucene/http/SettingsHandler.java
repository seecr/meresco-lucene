/* begin license *
 *
 * "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
 *
 * Copyright (C) 2015 Koninklijke Bibliotheek (KB) http://www.kb.nl
 * Copyright (C) 2015 Seecr (Seek You Too B.V.) http://seecr.nl
 *
 * This file is part of "Meresco Lucene"
 *
 * "Meresco Lucene" is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * "Meresco Lucene" is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with "Meresco Lucene"; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
 *
 * end license */

package org.meresco.lucene.http;

import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonNumber;
import javax.json.JsonObject;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.Similarity;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.meresco.lucene.Lucene;
import org.meresco.lucene.LuceneSettings;
import org.meresco.lucene.LuceneSettings.ClusterField;
import org.meresco.lucene.analysis.MerescoDutchStemmingAnalyzer;
import org.meresco.lucene.analysis.MerescoStandardAnalyzer;
import org.meresco.lucene.search.TermFrequencySimilarity;

public class SettingsHandler extends AbstractHandler {

    private Lucene lucene;

    public SettingsHandler(Lucene lucene) {
        //TODO: test this class
        this.lucene = lucene;
    }

    @Override
    public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
        try {
            LuceneSettings settings = lucene.getSettings();
            if (settings == null)
                settings = new LuceneSettings();
            if (request.getMethod() == "POST") {
                updateSettings(settings, request.getReader());
                if (lucene.getSettings() == null)
                    lucene.initSettings(settings);
            } else {
                response.setContentType("application/json");
                response.getWriter().write(settings.asJson().toString());
            }
        } catch (Exception e) {
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            response.getWriter().write(Utils.getStackTrace(e));
            baseRequest.setHandled(true);
            return;
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
                case "clusteringEps":
                    settings.clusteringEps = object.getJsonNumber(key).doubleValue();
                    break;
                case "clusteringMinPoints":
                    settings.clusteringMinPoints = object.getInt(key);
                    break;
                case "clusterMoreRecords":
                    settings.clusterMoreRecords  = object.getInt(key);
                    break;
                case "analyzer":
                    settings.analyzer = getAnalyzer(object.getJsonObject(key));
                    break;
                case "similarity":
                    settings.similarity = getSimilarity(object.getJsonObject(key));
                    break;
                case "drilldownFields":
                    updateDrilldownFields(settings.facetsConfig, object.getJsonArray(key));
                    break;
                case "clusterFields":
                    updateClusterFields(settings, object.getJsonArray(key));
                    break;
            }
        }

    }

    private static void updateClusterFields(LuceneSettings settings, JsonArray jsonClusterFields) {
        List<ClusterField> clusterFields = new ArrayList<ClusterField>();
        for (int i=0; i<jsonClusterFields.size(); i++) {
            JsonObject clusterField = jsonClusterFields.getJsonObject(i);
            String filterValue = clusterField.getString("filterValue", null);
            clusterFields.add(new ClusterField(clusterField.getString("fieldname"), clusterField.getJsonNumber("weight").doubleValue(), filterValue));
        }
        settings.clusterFields = clusterFields;
    }

    private static void updateDrilldownFields(FacetsConfig facetsConfig, JsonArray drilldownFields) {
        for (int i = 0; i < drilldownFields.size(); i++) {
            JsonObject drilldownField = drilldownFields.getJsonObject(i);
            String dim = drilldownField.getString("dim");
            if (drilldownField.get("hierarchical") != null)
                facetsConfig.setHierarchical(dim, drilldownField.getBoolean("hierarchical"));
            if (drilldownField.get("multiValued") != null)
                facetsConfig.setMultiValued(dim, drilldownField.getBoolean("multiValued"));
            String fieldname = drilldownField.getString("fieldname", null);
            if (fieldname != null && fieldname != null)
                facetsConfig.setIndexFieldName(dim, fieldname);
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
            case "TermFrequencySimilarity": // TODO: test
                return new TermFrequencySimilarity();
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
            case "WhitespaceAnalyzer":// TODO: test
                return new WhitespaceAnalyzer();
        }
        return null;
    }
}

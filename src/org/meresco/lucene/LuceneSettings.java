/* begin license *
 *
 * "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
 *
 * Copyright (C) 2015-2016 Koninklijke Bibliotheek (KB) http://www.kb.nl
 * Copyright (C) 2015-2016 Seecr (Seek You Too B.V.) http://seecr.nl
 * Copyright (C) 2016 Stichting Kennisnet http://www.kennisnet.nl
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

package org.meresco.lucene;

import java.io.Reader;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonNumber;
import javax.json.JsonObject;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.index.LogMergePolicy;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.Similarity;
import org.meresco.lucene.analysis.MerescoDutchStemmingAnalyzer;
import org.meresco.lucene.analysis.MerescoStandardAnalyzer;
import org.meresco.lucene.search.InterpolateEpsilon;
import org.meresco.lucene.search.TermFrequencySimilarity;


public class LuceneSettings {
    public Similarity similarity = new BM25Similarity();
    public Analyzer analyzer = new MerescoStandardAnalyzer();
    private MergePolicy mergePolicy = this.getTieredMergePolicy(8.0, 2);
    public int lruTaxonomyWriterCacheSize = 4000;
    public int numberOfConcurrentTasks = 6;
    public int commitTimeout = 10;
    public int commitCount = 100000;
    public FacetsConfig facetsConfig = new FacetsConfig() {
        @Override
        protected DimConfig getDefaultDimConfig() {
            DEFAULT_DIM_CONFIG.multiValued = true;
            return DEFAULT_DIM_CONFIG;
        }
    };
    public ClusterConfig clusterConfig = new ClusterConfig(0.4, 1, 100);
    public InterpolateEpsilon interpolateEpsilon = new InterpolateEpsilon();
    public boolean cacheFacetOrdinals = true;

    public JsonObject asJson() {
        JsonArrayBuilder strategiesJsonBuilder = Json.createArrayBuilder();
        for (ClusterStrategy strategy : this.clusterConfig.strategies) {
            strategiesJsonBuilder.add(Json.createObjectBuilder()
                    .add("clusteringEps", strategy.clusteringEps)
                    .add("clusteringMinPoints", strategy.clusteringMinPoints));
        }

        JsonObject json = Json.createObjectBuilder()
                .add("similarity", similarity.toString())
                .add("mergePolicy", this.mergePolicy instanceof TieredMergePolicy
                        ? Json.createObjectBuilder()
                                .add("type", "TieredMergePolicy")
                                .add("maxMergeAtOnce", ((TieredMergePolicy) this.mergePolicy).getMaxMergeAtOnce())
                                .add("segmentsPerTier", ((TieredMergePolicy) this.mergePolicy).getSegmentsPerTier())
                        : Json.createObjectBuilder()
                                .add("type", "LogDocMergePolicy")
                                .add("maxMergeDocs", ((LogMergePolicy) this.mergePolicy).getMaxMergeDocs())
                                .add("mergeFactor", ((LogMergePolicy) this.mergePolicy).getMergeFactor()))
                .add("lruTaxonomyWriterCacheSize", lruTaxonomyWriterCacheSize)
                .add("numberOfConcurrentTasks", numberOfConcurrentTasks)
                .add("commitCount", commitCount)
                .add("commitTimeout", commitTimeout)
                .add("cacheFacetOrdinals", this.cacheFacetOrdinals)
                .add("clustering", Json.createObjectBuilder()
                        .add("clusterMoreRecords", clusterConfig.clusterMoreRecords)
                        .add("strategies", strategiesJsonBuilder))
                .build();
        return json;
    }

    public void updateSettings(Reader reader) throws Exception {
        JsonObject object = (JsonObject) Json.createReader(reader).read();
        for (String key : object.keySet()) {
            switch (key) {
            case "commitCount":
                commitCount = object.getInt(key);
                break;
            case "commitTimeout":
                commitTimeout = object.getInt(key);
                break;
            case "lruTaxonomyWriterCacheSize":
                lruTaxonomyWriterCacheSize = object.getInt(key);
                break;
            case "mergePolicy":
                JsonObject policy = object.getJsonObject("mergePolicy");
                switch (policy.getString("type")) {
                case "TieredMergePolicy":
                    this.mergePolicy = getTieredMergePolicy(policy.getJsonNumber("segmentsPerTier").doubleValue(), policy.getInt("maxMergeAtOnce"));
                    break;
                case "LogDocMergePolicy": {
                    LogDocMergePolicy mp;
                    this.mergePolicy = mp = new LogDocMergePolicy();
                    mp.setMaxMergeDocs(policy.getInt("maxMergeDocs"));
                    mp.setMergeFactor(policy.getInt("mergeFactor"));
                    break;
                }
                default:
                    throw new RuntimeException("Unsupported mergePolicy: " + policy.getString("type"));
                }
                break;
            case "numberOfConcurrentTasks":
                numberOfConcurrentTasks = object.getInt(key);
                break;
            case "cacheFacetOrdinals":
                this.cacheFacetOrdinals = object.getBoolean("cacheFacetOrdinals");
                break;
            case "analyzer":
                analyzer = getAnalyzer(object.getJsonObject(key));
                break;
            case "similarity":
                similarity = getSimilarity(object.getJsonObject(key));
                break;
            case "drilldownFields":
                updateDrilldownFields(facetsConfig, object.getJsonArray(key));
                break;
            case "clustering":
                ClusterConfig clusterConfig = ClusterConfig.parseFromJsonObject(object.getJsonObject(key));
                if (clusterConfig != null) {
                    this.clusterConfig = clusterConfig;
                }
                break;
            }
        }
    }

    private MergePolicy getTieredMergePolicy(double segmentsPerTier, int maxMergeAtOnce) {
        TieredMergePolicy mp = new TieredMergePolicy();
        return mp.setMaxMergeAtOnce(maxMergeAtOnce).setSegmentsPerTier(segmentsPerTier);
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
        case "TermFrequencySimilarity":
            return new TermFrequencySimilarity();
        }
        return null;
    }

    private static Analyzer getAnalyzer(JsonObject analyzer) {
        switch (analyzer.getString("type")) {
        case "MerescoDutchStemmingAnalyzer":
            JsonArray jsonFields = analyzer.getJsonArray("stemmingFields");
            String[] fields = new String[jsonFields.size()];
            for (int i = 0; i < jsonFields.size(); i++) {
                fields[i] = jsonFields.getString(i);
            }
            return new MerescoDutchStemmingAnalyzer(fields);
        case "MerescoStandardAnalyzer":
            return new MerescoStandardAnalyzer();
        case "WhitespaceAnalyzer":
            return new WhitespaceAnalyzer();
        }
        return null;
    }

    public MergePolicy getMergePolicy() {
        return this.mergePolicy;
    }
}

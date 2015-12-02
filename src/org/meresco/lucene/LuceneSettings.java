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

package org.meresco.lucene;

import java.util.ArrayList;
import java.util.List;

import javax.json.Json;
import javax.json.JsonObject;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.Similarity;
import org.meresco.lucene.analysis.MerescoStandardAnalyzer;

public class LuceneSettings {

    public static class ClusterField {
        public String fieldname;
        public double weight;
        public String filterValue;

        public ClusterField(String fieldname, double weight, String filterValue) {
            this.fieldname = fieldname;
            this.weight = weight;
            this.filterValue = filterValue;
        }
    }

    public Similarity similarity = new BM25Similarity();
    public Analyzer analyzer = new MerescoStandardAnalyzer();
    public int maxMergeAtOnce = 2;
    public double segmentsPerTier = 8.0;
    public int lruTaxonomyWriterCacheSize = 4000;
    public int numberOfConcurrentTasks = 6;
    public int commitTimeout = 10;
    public int commitCount = 100000;
    public FacetsConfig facetsConfig = new FacetsConfig();
    public double clusteringEps = 0.4;
    public int clusteringMinPoints = 1;
    public int clusterMoreRecords = 100;
    public List<ClusterField> clusterFields = new ArrayList<>();

    public JsonObject asJson() {
        JsonObject json = Json.createObjectBuilder()
            .add("similarity", similarity.toString())
            .add("maxMergeAtOnce", maxMergeAtOnce)
            .add("segmentsPerTier", segmentsPerTier)
            .add("lruTaxonomyWriterCacheSize", lruTaxonomyWriterCacheSize)
            .add("numberOfConcurrentTasks", numberOfConcurrentTasks)
            .add("commitCount", commitCount)
            .add("commitTimeout", commitTimeout)
            .add("clusteringEps", clusteringEps)
            .add("clusteringMinPoints", clusteringMinPoints)
            .add("clusterMoreRecords", clusterMoreRecords)
            .build();
        return json;
    }
}

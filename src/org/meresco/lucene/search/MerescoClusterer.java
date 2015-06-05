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

package org.meresco.lucene.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.math3.ml.clustering.Cluster;
import org.apache.commons.math3.ml.clustering.DBSCANClusterer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;
import org.meresco.lucene.search.PageRank.Node;

public class MerescoClusterer {

    private IndexReader reader;
    private Map<String, Double> fieldsWeight = new HashMap<String, Double>();
    private List<MerescoVector> docvectors = new ArrayList<MerescoVector>();
    private BytesRefHash ords = new BytesRefHash();
    public List<Cluster<MerescoVector>> clusters;
    private double eps;
    private int minPoints;

    public MerescoClusterer(IndexReader reader, double eps) {
        this(reader, eps, 1);
    }

    public MerescoClusterer(IndexReader reader, double eps, int minPoints) {
        this.reader = reader;
        this.eps = eps;
        this.minPoints = minPoints;
    }

    public void registerField(String fieldname, double weight) {
        this.fieldsWeight.put(fieldname, weight);
    }

    public void collect(int doc) throws IOException {
        MerescoVector vector = this.createVector(doc);
        if (vector != null) {
            this.docvectors.add(vector);
        }
    }

    public void processTopDocs(TopDocs topDocs) throws IOException {
        for (ScoreDoc scoreDoc : topDocs.scoreDocs)
            this.collect(scoreDoc.doc);
    }

    public void finish() {
        this.clusters = new DBSCANClusterer<MerescoVector>(this.eps, this.minPoints, new GeneralizedJaccardDistance()).cluster(this.docvectors);
    }

    private MerescoCluster rankCluster(List<MerescoVector> vectors) {
        PageRank pageRank = new PageRank(this.ords.size());
        for (MerescoVector vector : vectors) {
            pageRank.add(vector.docId, vector.getPoint());
        }
        pageRank.prepare();
        for (int i = 0; i < 5; i++)
            pageRank.iterate();
        MerescoCluster.DocScore[] topDocs = new MerescoCluster.DocScore[vectors.size()];
        int i = 0;
        for (PageRank.Node n : pageRank.topDocs()) {
            topDocs[i++] = new MerescoCluster.DocScore(n.id, n.getPR());
        }

        i = 0;
        List<Node> rankedTerms = pageRank.topTerms();
        MerescoCluster.TermScore[] topTerms = new MerescoCluster.TermScore[rankedTerms.size()];
        for (PageRank.Node n : rankedTerms) {
            BytesRef ref = new BytesRef();
            this.ords.get(n.id, ref);
            topTerms[i++] = new MerescoCluster.TermScore(ref.utf8ToString(), n.getPR());
        }
        return new MerescoCluster(topDocs, topTerms);
    }

    public MerescoCluster cluster(int docId) {
        for (Cluster<MerescoVector> c : this.clusters) {
            List<MerescoVector> points = c.getPoints();
            for (MerescoVector oc : points) {
                if (oc.docId == docId) {
                    return rankCluster(c.getPoints());
                }
            }
        }
        return null;
    }

    private MerescoVector createVector(int docId) throws IOException {
        MerescoVector vector = null;
        double vectorWeight = 1.0;
        for (String fieldname : this.fieldsWeight.keySet()) {
            MerescoVector v = this.termVector(docId, fieldname);
            if (v != null) {
                double weight = this.fieldsWeight.get(fieldname);
                if (vector == null) {
                    vector = v;
                    vectorWeight = weight;
                } else {
                    vector.combineToSelf(vectorWeight, weight, v);
                    vectorWeight = 1;
                }
            }
        }
        if (vector == null) {
            return null;
        }
        return vector;
    }

    private MerescoVector termVector(final int docId, String field) throws IOException {
        Terms terms = this.reader.getTermVector(docId, field);
        if (terms == null)
            return null;
        TermsEnum termsEnum = terms.iterator(null);
        MerescoVector vector = new MerescoVector(docId);
        while (termsEnum.next() != null) {
            BytesRef term = termsEnum.term();
            vector.setEntry(ord(term), termsEnum.totalTermFreq());
        }
        return vector;
    }

    private int ord(BytesRef b) {
        int ord = ords.add(b);
        if (ord < 0)
            ord = -ord - 1;
        return ord;
    }

    public void printClusters() {
        ClusterClusterer cc = new ClusterClusterer(this.ords, this.clusters);
        cc.print();
    }
}

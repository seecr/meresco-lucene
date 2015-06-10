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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

import org.apache.commons.math3.ml.clustering.Cluster;
import org.apache.commons.math3.ml.clustering.DBSCANClusterer;
import org.apache.commons.math3.util.OpenIntToDoubleHashMap;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
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
    private Map<Integer, Integer> jointCounts = new HashMap<Integer, Integer>();
    private OpenIntToDoubleHashMap marginalProbs = new OpenIntToDoubleHashMap();
    private int numHits;
    private int numDocs;
    private Set<Integer> relevantTerms = new HashSet<Integer>();

    public MerescoClusterer(IndexReader reader, double eps) {
        this(reader, eps, 1);
    }

    public MerescoClusterer(IndexReader reader, double eps, int minPoints) {
        this.reader = reader;
        this.eps = eps;
        this.minPoints = minPoints;
        this.numDocs = reader.numDocs();
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

    static private class MITerm implements Comparable<MITerm> {

        double mi;
        String term;
        int ord;

        public MITerm(double mi, String term, int ord) {
            this.mi = mi;
            this.term = term;
            this.ord = ord;
        }

        @Override
        public int compareTo(MITerm rhs) {
            return -Double.compare(this.mi, rhs.mi);
        }
    }

    public void processTopDocs(TopDocs topDocs) throws IOException {
        this.numHits = topDocs.totalHits;
        for (ScoreDoc scoreDoc : topDocs.scoreDocs)
            this.collect(scoreDoc.doc);
        findMostRelevantTerms();
    }

    private void findMostRelevantTerms() {
        PriorityQueue<MITerm> q = new PriorityQueue<MITerm>();
        for (int i = 0; i < this.ords.size(); i++) {
            BytesRef br = new BytesRef();
            this.ords.get(i, br);
            double mi = this.calc_mi(i);
            q.add(new MITerm(mi, br.utf8ToString(), i));
        }
        for (int i = 0; i < Math.min(100, q.size()); i++) {
            MITerm mit = q.poll();
            this.relevantTerms.add(mit.ord);
            System.out.println(i + ":  " + mit.mi + " => " + mit.term);
        }
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

    public List<String> allTerms() {
        List<String> terms = new ArrayList<String>();
        BytesRef b = new BytesRef();
        for (int i = 0; i < this.ords.size(); i++) {
            this.ords.get(i, b);
            terms.add(b.utf8ToString());
        }
        return terms;
    }

    private MerescoVector termVector(final int docId, String field) throws IOException {
        Terms terms = this.reader.getTermVector(docId, field);
        if (terms == null)
            return null;
        TermsEnum termsEnum = terms.iterator(null);
        MerescoVector vector = new MerescoVector(docId, this.ords, this.relevantTerms);
        while (termsEnum.next() != null) {
            BytesRef term = termsEnum.term();
            int ord = register(term);
            vector.setEntry(ord, termsEnum.totalTermFreq());
        }
        return vector;
    }

    private int register(BytesRef b) {
        int ord = ords.add(b);
        if (ord < 0)
            ord = -ord - 1;
        Integer count = this.jointCounts.get(ord);
        if (count == null)
            count = 0;
        this.jointCounts.put(ord, ++count);
        if (!this.marginalProbs.containsKey(ord))
            try {
                double marginalProbability = this.reader.docFreq(new Term("__all__", b)) / (double) this.numDocs;
                this.marginalProbs.put(ord, marginalProbability);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        return ord;
    }

    private double calc_mi(int ord) {
        double p_x = this.marginalProbs.get(ord);
        double p_y = this.numHits / (double) this.numDocs;
        double p_x_y = this.jointCounts.get(ord) / (double) this.ords.size();
        double mi = p_x_y * Math.log(p_x_y / (p_x * p_y));
        return mi;
    }

    public void printClusters() {
        ClusterClusterer cc = new ClusterClusterer(this.ords, this.clusters);
        cc.print();
    }
}

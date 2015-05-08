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

import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.ml.clustering.Cluster;
import org.apache.commons.math3.ml.clustering.Clusterable;
import org.apache.commons.math3.ml.clustering.DBSCANClusterer;
import org.apache.commons.math3.util.OpenIntToDoubleHashMap;
import org.apache.commons.math3.util.OpenIntToDoubleHashMap.Iterator;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.NumericUtils;

public class MerescoClusterer {

    private static final double FAR_FAR_AWAY = 0.7;
    private static final double CLOSE_PROXIMITY = 0.5;
    private IndexReader reader;
    private Map<String, Double> fieldsWeight = new HashMap<String, Double>();
    private List<MerescoVector> docvectors = new ArrayList<MerescoVector>();
    private BytesRefHash ords = new BytesRefHash();
    public List<Cluster<MerescoVector>> clusters;
    private double eps;
    private int minPoints;
    private List<String> numericFields = new ArrayList<String>();

    public MerescoClusterer(IndexReader reader, double eps) {
        this(reader, eps, 1);
    }

    public MerescoClusterer(IndexReader reader, double eps, int minPoints) {
        this.reader = reader;
        this.eps = eps;
        this.minPoints = minPoints;
    }

    public void registerField(String fieldname, double weight, boolean numeric) {
        this.fieldsWeight.put(fieldname, weight);
        if (numeric)
            this.numericFields.add(fieldname);
    }

    public void collect(int doc) throws IOException {
        MerescoVector vector = createVector(doc);
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
        // System.out.println("Ords: " + this.ords.size());
    }

    private int[] rankCluster(List<MerescoVector> vectors) {
        PageRank pageRank = new PageRank(this.ords.size());
        for (MerescoVector vector : vectors) {
            pageRank.add(vector.docId, vector.getPoint());
        }
        pageRank.prepare();
        pageRank.iterate();
        int[] result = new int[vectors.size()];
        int i = 0;
        for (PageRank.Node n : pageRank.topDocs()) {
            result[i++] = n.id;
        }
        return result;
    }

    public int[] cluster(int docId) {
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
        if (this.numericFields.contains(field)) {
            return this.vectorFromNumericField(docId, field);
        } else {
            return this.vectorFromTermVector(docId, field);
        }
    }

    private MerescoVector vectorFromTermVector(final int docId, String field) throws IOException {
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

    private MerescoVector vectorFromNumericField(final int docId, String field) throws IOException {
        List<AtomicReaderContext> leaves = this.reader.leaves();
        AtomicReaderContext context = leaves.get(ReaderUtil.subIndex(docId, leaves));
        NumericDocValues docValues = context.reader().getNumericDocValues(field);
        if (docValues == null) {
            return null;
        }
        BytesRefBuilder term = new BytesRefBuilder();
        long docValue = docValues.get(docId - context.docBase);
        if (docValue == 0) {
            return null;
        }
        NumericUtils.longToPrefixCoded(docValue, 0, term);
        MerescoVector vector = new MerescoVector(docId);
        vector.setEntry(ord(term.get()), 1);
        return vector;
    }

    public int ord(BytesRef b) {
        int ord = ords.add(b);
        if (ord < 0)
            ord = -ord - 1;
        return ord;
    }

    public void printClusters() {
        System.out.println("Aantal clusters: " + this.clusters.size());
        List<Cluster<MerescoVector>> clusters = new ArrayList<Cluster<MerescoVector>>(this.clusters);
        if (clusters.isEmpty())
            return;
        Cluster<MerescoVector> cl = clusters.remove(0);
        while (cl != null) {
            System.out.println("===");
            print_cluster(cl);
            System.out.println("===");
            cl = getFarthestCluster(cl, clusters);
        }
    }

    private void print_cluster(Cluster<MerescoVector> cl) {
        for (MerescoVector v : cl.getPoints()) {
            v.printVector(ords);
        }
    }

    private Cluster<MerescoVector> getFarthestCluster(Cluster<MerescoVector> c, List<Cluster<MerescoVector>> clusters) {
        MySumOfClusterVariances eval = new MySumOfClusterVariances(new GeneralizedJaccardDistance());
        Cluster<MerescoVector> closest_c = null;
        Cluster<MerescoVector> farthest_c = null;
        MerescoVector self = eval.centroidOf(c);
        double farthest_d = FAR_FAR_AWAY;
        double closest_d = CLOSE_PROXIMITY;
        for (Cluster<MerescoVector> other : clusters) {
            MerescoVector rhs = eval.centroidOf(other);
            double distance = eval.distance(self, rhs);
            if (distance > farthest_d) {
                farthest_c = other;
                farthest_d = distance;
            }
            if (distance < closest_d) {
                closest_d = distance;
                closest_c = other;
            }
        }
        if (closest_d < CLOSE_PROXIMITY) {
            System.out.println("IGNORING: " + closest_d);
            eval.centroidOf(closest_c).printVector(ords);
            clusters.remove(closest_c);
        }
        clusters.remove(farthest_c);
        System.out.println("closest: " + closest_d + "  farthest: " + farthest_d + "  (remaining: " + clusters.size() + ")");
        return farthest_c;
    }

    static class MerescoVector implements Clusterable {
        private OpenIntToDoubleHashMap entries;
        private int docId;
        private int maxIndex;
        private ArrayRealVector point = null;

        public MerescoVector(int docId) {
            this.entries = new OpenIntToDoubleHashMap(0.0);
            this.docId = docId;
            this.maxIndex = 0;
        }

        public MerescoVector() {
            this(-1);
        }

        public void setEntry(int index, double value) {
            this.entries.put(index, value);
            if (index > this.maxIndex)
                this.maxIndex = index;
        }

        public void combineToSelf(double a, double b, MerescoVector y) {
            int maxIndex = Math.max(this.maxIndex, y.maxIndex);
            for (int i = 0; i <= maxIndex; i++) {
                final double xi = this.entries.get(i);
                final double yi = y.entries.get(i);
                setEntry(i, a * xi + b * yi);
            }
        }

        public double[] getPoint() {
            if (this.point == null) {
                this.point = new ArrayRealVector(this.maxIndex + 1);
                Iterator iter = entries.iterator();
                while (iter.hasNext()) {
                    iter.advance();
                    this.point.setEntry(iter.key(), iter.value());
                }
            }
            this.point.unitize();
            return this.point.getDataRef();
        }

        public int docId() {
            return this.docId;
        }

        public void printVector(BytesRefHash hash) {
            Iterator iter = entries.iterator();
            while (iter.hasNext()) {
                iter.advance();
                if (iter.value() > 0) {
                    BytesRef b = new BytesRef();
                    hash.get(iter.key(), b);
                    System.out.print(b.utf8ToString() + ":" + iter.value() + "  ");
                }
            }
            System.out.println();
        }
    }
}
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

    private IndexReader reader;
    private Map<String, Double> fieldnames = new HashMap<String, Double>();
    private List<MerescoVector> docvectors = new ArrayList<MerescoVector>();
    private BytesRefHash ords = new BytesRefHash();
    private List<Cluster<MerescoVector>> cluster;
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
        this.fieldnames.put(fieldname, weight);
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
        this.cluster = new DBSCANClusterer<MerescoVector>(this.eps, this.minPoints, new InverseJaccardDistance())
                .cluster(this.docvectors);
        // System.out.println("Ords: " + this.ords.size());
    }

    public void printClusters() {
        for (Cluster<MerescoVector> c : this.cluster) {
            System.out.println(" -- cluster --");
            for (MerescoVector v : c.getPoints()) {
                v.printVector(this.ords);
            }
        }
    }

    public int[] cluster(int docId) {
        for (Cluster<MerescoVector> c : this.cluster) {
            List<MerescoVector> points = c.getPoints();
            for (MerescoVector oc : points) {
                if (oc.docId == docId) {
                    int[] result = new int[points.size()];
                    int i = 0;
                    for (MerescoVector oc1 : points) {
                        result[i++] = oc1.docId;
                    }
                    return result;
                }
            }
        }
        return null;
    }

    private MerescoVector createVector(int docId) throws IOException {
        MerescoVector vector = null;
        double vectorWeight = 1.0;
        for (String fieldname : fieldnames.keySet()) {
            MerescoVector v = this.termVector(docId, fieldname);
            if (v != null) {
                double weight = this.fieldnames.get(fieldname);
                if (vector == null) {
                    vector = v;
                    vectorWeight = weight;
                } else {
                    vector.combineToSelf(vectorWeight, weight, v);
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
        NumericUtils.longToPrefixCoded(docValues.get(docId - context.docBase), 0, term);
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

    static class MerescoVector implements Clusterable {
        private OpenIntToDoubleHashMap entries;
        private int docId;
        private int maxIndex;
        private double[] point = null;

        public MerescoVector(int docId) {
            this.entries = new OpenIntToDoubleHashMap(0.0);
            this.docId = docId;
            this.maxIndex = 0;
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

        public void setEntry(int index, double value) {
            this.entries.put(index, value);
            if (index > this.maxIndex)
                this.maxIndex = index;
        }

        public void combineToSelf(double a, double b, MerescoVector y) {
            int maxSize = Math.max(this.maxIndex, y.maxIndex);
            for (int i = 0; i < maxSize; i++) {
                final double xi = this.entries.get(i);
                final double yi = y.entries.get(i);
                setEntry(i, a * xi + b * yi);
            }
        }

        public double[] getPoint() {
            if (this.point == null) {
                this.point = new double[this.maxIndex + 1];
                Iterator iter = entries.iterator();
                while (iter.hasNext()) {
                    iter.advance();
                    this.point[iter.key()] = iter.value();
                }
            }
            return this.point;
        }

        public int docId() {
            return this.docId;
        }
    }
}
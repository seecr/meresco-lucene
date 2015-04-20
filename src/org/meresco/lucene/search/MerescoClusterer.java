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

import org.apache.commons.math3.linear.OpenMapRealVector;
import org.apache.commons.math3.linear.RealVector;
import org.apache.commons.math3.ml.clustering.Cluster;
import org.apache.commons.math3.ml.clustering.Clusterable;
import org.apache.commons.math3.ml.clustering.DBSCANClusterer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;

public class MerescoClusterer {

    private IndexReader reader;
    private Map<String, Double> fieldnames = new HashMap<String, Double>();
    private List<MerescoVector> clusters = new ArrayList<MerescoVector>();
    private BytesRefHash ords = new BytesRefHash();
    private List<Cluster<MerescoVector>> cluster;
    private double eps;
    private int minPoints;

    public MerescoClusterer(IndexReader reader, double eps) {
        this(reader, eps, 2);
    }

    public MerescoClusterer(IndexReader reader, double eps, int minPoints) {
        this.reader = reader;
        this.eps = eps;
        this.minPoints = minPoints;
    }

    public void registerField(String fieldname, double weight) {
        fieldnames.put(fieldname, weight);
    }
    
    public void collect(int doc) throws IOException {
        MerescoVector vector = createVector(doc);   
        if (vector != null)
            this.clusters.add(vector);
    }

    public void processTopDocs(TopDocs topDocs) throws IOException {
        for (ScoreDoc scoreDoc : topDocs.scoreDocs)
            this.collect(scoreDoc.doc);
        this.finish();
    }

    public void finish() {
        this.cluster = new DBSCANClusterer<MerescoVector>(this.eps, this.minPoints).cluster(this.clusters);
    }

    public int[] cluster(int docId) {
        for (Cluster<MerescoVector> c : this.cluster) {
            List<MerescoVector> points = c.getPoints();
            for (MerescoVector oc : points) {
                if (oc.docId == docId) {
                    int[] result = new int[points.size()];
                    int i=0;
                    for (MerescoVector oc1 : points) {
                        result[i++] = oc1.docId;
                    }
                    return result;
                }
            }
        }
        return null;
    }

    public MerescoVector createVector(int docId) throws IOException {
        RealVector vector = null;
        double vectorWeight = 1.0;
        for (String fieldname : fieldnames.keySet()) {
            RealVector v = termVector(docId, fieldname);
            if (v != null) {
                if (vector == null) {
                    vector = v;
                    vectorWeight = this.fieldnames.get(fieldname);
                } else {
                    vector.combineToSelf(vectorWeight, this.fieldnames.get(fieldname), v);
                }
            }
        }
        if (vector == null) {
            return null;
        }
        return new MerescoVector(vector, docId);
    }
    
    public RealVector termVector(final int docId, String field) throws IOException {
        Terms terms = this.reader.getTermVector(docId, field);
        if (terms == null)
            return null;
        TermsEnum termsEnum = terms.iterator(null);
        final RealVector vector = new OpenMapRealVector(10000);
        while (termsEnum.next() != null) {
            BytesRef term = termsEnum.term();
            int ord = ords.add(term);
            if (ord < 0)
                ord = -ord - 1;
            vector.setEntry(ord, termsEnum.totalTermFreq());
        }
        return vector;
    }

    class MerescoVector implements Clusterable {
        private RealVector vector;
        private int docId;

        public MerescoVector(RealVector vector, int docId) {
            this.vector = vector;
            this.docId = docId;
        }

        @Override
        public double[] getPoint() {
            return this.vector.toArray();
        }

        public int docId() {
            return this.docId;
        }
    }
}
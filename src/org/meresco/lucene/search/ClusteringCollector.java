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
import java.util.List;

import org.apache.commons.math3.linear.OpenMapRealVector;
import org.apache.commons.math3.linear.RealVector;
import org.apache.commons.math3.ml.clustering.CentroidCluster;
import org.apache.commons.math3.ml.clustering.Clusterable;
import org.apache.commons.math3.ml.clustering.KMeansPlusPlusClusterer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;

public class ClusteringCollector {

    private IndexReader reader;
    private String fieldname;
    private List<OurClusterable> clusters = new ArrayList<OurClusterable>();
    private BytesRefHash ords = new BytesRefHash();
    private List<CentroidCluster<OurClusterable>> cluster;
    private int clusterSize;

    public ClusteringCollector(IndexReader reader, String fieldname, int clusterSize) {
        this.reader = reader;
        this.fieldname = fieldname;
        this.clusterSize = clusterSize;
    }

    public void collect(int doc) throws IOException {
        OurClusterable tv = termVector(doc, fieldname);
        this.clusters.add(tv);
    }

    public void finish() {
        this.cluster = new KMeansPlusPlusClusterer<OurClusterable>(this.clusterSize).cluster(this.clusters);
    }

    public int[] cluster(int docId) {
        for (CentroidCluster<OurClusterable> c : this.cluster) {
            List<OurClusterable> points = c.getPoints();
            for (OurClusterable oc : points) {
                if (oc.docId == docId) {
                    int[] result = new int[points.size()];
                    int i=0;
                    for (OurClusterable oc1 : points) {
                        result[i++] = oc1.docId;
                    }
                    return result;
                }
            }
        }
        return null;
    }

    public OurClusterable termVector(final int docId, String field) throws IOException {
        Terms terms = this.reader.getTermVector(docId, field);
        TermsEnum termsEnum = terms.iterator(null);
        final RealVector tv = new OpenMapRealVector(10000);
        while (termsEnum.next() != null) {
            BytesRef term = termsEnum.term();
            int ord = ords.add(term);
            if (ord < 0)
                ord = -ord - 1;
            tv.setEntry(ord, termsEnum.totalTermFreq());
        }
        return new OurClusterable(tv, docId);
    }

    public double cosSimilarity(RealVector vector1, RealVector vector2) throws IOException {
        return vector1.dotProduct(vector2);
    }

    class OurClusterable implements Clusterable {
        private RealVector tv;
        private int docId;

        public OurClusterable(RealVector tv, int docId) {
            this.tv = tv;
            this.docId = docId;
        }

        @Override
        public double[] getPoint() {
            return this.tv.toArray();
        }

        public int docId() {
            return this.docId;
        }
    }
}
package org.meresco.lucene.search;


public class EpsilonInterpolator {
    public double interpolateEpsilon(int totalHits, int sliceSize, double clusteringEps, int clusterMoreRecords) {
        double eps = clusteringEps * (totalHits - sliceSize) / clusterMoreRecords;
        return Math.max(Math.min(eps, clusteringEps), 0.0);
    }
}

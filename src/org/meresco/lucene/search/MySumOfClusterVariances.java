package org.meresco.lucene.search;

import org.apache.commons.math3.ml.clustering.Cluster;
import org.apache.commons.math3.ml.clustering.Clusterable;
import org.apache.commons.math3.ml.clustering.evaluation.SumOfClusterVariances;
import org.apache.commons.math3.ml.distance.DistanceMeasure;
import org.meresco.lucene.search.MerescoClusterer.MerescoVector;

public class MySumOfClusterVariances extends SumOfClusterVariances<MerescoVector> {

    @Override
    public MerescoVector centroidOf(Cluster<MerescoVector> arg0) {
        MerescoVector centroid = new MerescoVector();
        for (MerescoVector v : arg0.getPoints()) {
            centroid.combineToSelf(1.0, 1.0/arg0.getPoints().size(), v);
        }
        return centroid;
    }

    public MySumOfClusterVariances(DistanceMeasure measure) {
        super(measure);
    }

    @Override
    public double distance(Clusterable p1, Clusterable p2) {
        return super.distance(p1, p2);
    }
}

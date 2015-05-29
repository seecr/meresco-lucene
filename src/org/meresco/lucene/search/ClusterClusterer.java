package org.meresco.lucene.search;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.math3.ml.clustering.Cluster;
import org.apache.lucene.util.BytesRefHash;

public class ClusterClusterer {
    private static final double FAR_FAR_AWAY = 0.7;
    private static final double CLOSE_PROXIMITY = 0.5;
    private BytesRefHash ords;
    private Collection<? extends Cluster<MerescoVector>> clusters;

    public ClusterClusterer(BytesRefHash ords, List<Cluster<MerescoVector>> clusters) {
        this.ords = ords;
        this.clusters = clusters;
    }

    private void print_cluster(Cluster<MerescoVector> cl) {
        for (MerescoVector v : cl.getPoints()) {
            v.printVector(this.ords);
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

    public void print() {
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

}

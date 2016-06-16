package org.meresco.lucene.search;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.meresco.lucene.ClusterConfig;
import org.meresco.lucene.LuceneSettings;


public class EpsilonInterpolatorTest {
    @Test
    public void testInterpolateEps() throws Exception {
    	ClusterConfig clusterConfig = new LuceneSettings().clusterConfig;
    	int clusterMoreRecords = clusterConfig.clusterMoreRecords;
    	double clusteringEps = clusterConfig.strategies.get(0).clusteringEps;
    	
    	InterpolateEpsilon interpolator = new InterpolateEpsilon();
        assertEquals(0, interpolator.interpolateEpsilon(0, 10, clusteringEps, clusterMoreRecords), 0);
        assertEquals(0, interpolator.interpolateEpsilon(10, 10, clusteringEps, clusterMoreRecords), 0);
        assertEquals(0.004, interpolator.interpolateEpsilon(11, 10, clusteringEps, clusterMoreRecords), 0);
        assertEquals(0.4, interpolator.interpolateEpsilon(110, 10, clusteringEps, clusterMoreRecords), 0);
        assertEquals(0.4, interpolator.interpolateEpsilon(111, 10, clusteringEps, clusterMoreRecords), 0);

        assertEquals(0, interpolator.interpolateEpsilon(0, 20, clusteringEps, clusterMoreRecords), 0);
        assertEquals(0, interpolator.interpolateEpsilon(20, 20, clusteringEps, clusterMoreRecords), 0);
        assertEquals(0.004, interpolator.interpolateEpsilon(21, 20, clusteringEps, clusterMoreRecords), 0);
        assertEquals(0.32, interpolator.interpolateEpsilon(100, 20, clusteringEps, clusterMoreRecords), 0);
        assertEquals(0.4, interpolator.interpolateEpsilon(120, 20, clusteringEps, clusterMoreRecords), 0);
        assertEquals(0.4, interpolator.interpolateEpsilon(121, 20, clusteringEps, clusterMoreRecords), 0);
    }
}


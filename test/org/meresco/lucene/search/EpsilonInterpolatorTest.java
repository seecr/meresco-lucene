/* begin license *
 *
 * "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
 *
 * Copyright (C) 2016 Koninklijke Bibliotheek (KB) http://www.kb.nl
 * Copyright (C) 2016 Seecr (Seek You Too B.V.) http://seecr.nl
 * Copyright (C) 2016 Stichting Kennisnet http://www.kennisnet.nl
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


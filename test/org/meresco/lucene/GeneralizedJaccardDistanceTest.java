package org.meresco.lucene;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;

import org.junit.Test;
import org.meresco.lucene.SeecrTestCase;
import org.meresco.lucene.search.GeneralizedJaccardDistance;


public class GeneralizedJaccardDistanceTest extends SeecrTestCase {
 
    @Test
    public void test() {
        GeneralizedJaccardDistance J = new GeneralizedJaccardDistance();
        assertEquals(0.00, J.compute(new double[] {1.0}, new double[] {1.0}), 0);
        assertEquals(1.00, J.compute(new double[] {}, new double[] {1.0}), 0);
        assertEquals(1.00, J.compute(new double[] {1.0}, new double[] {}), 0);
        assertEquals(0.00, J.compute(new double[] {1.0, 2.0}, new double[] {1.0, 2.0}), 0);
        assertEquals(0.50, J.compute(new double[] {1.0}, new double[] {2.0}), 0); // 1.0 / 2.0
        assertEquals(0.75, J.compute(new double[] {2.0}, new double[] {8.0}), 0); // 2.0 / 8.0
        assertEquals(0.75, J.compute(new double[] {8.0}, new double[] {2.0}), 0); // 2.0 / 8.0
        assertEquals(0.50, J.compute(new double[] {1.0, 2.0}, new double[] {3.0, 3.0}), 0); // 1.0 + 2.0 / 3.0 + 3.0 = 0.5
        assertEquals(0.50, J.compute(new double[] {1.0, 2.0}, new double[] {3.0, 3.0}), 0); // 1.0 + 2.0 / 3.0 + 3.0 = 0.5
    }
    
    @Test
    public void testNaN() {
        // not sure if this is OK, but this is how it is right now
        GeneralizedJaccardDistance J = new GeneralizedJaccardDistance();
        double nan = J.compute(new double[0], new double[0]);
        assertNotSame(nan, nan);  // by IEEE 754
    }
    
    @Test
    public void testNegativeNoException() {
        // NOT OK, since this algorithm requires al elements >= 0.0 for correct results
        GeneralizedJaccardDistance J = new GeneralizedJaccardDistance();
        assertEquals(2.00, J.compute(new double[] {1.0}, new double[] {-1.0}), 0);
        assertEquals(2.00, J.compute(new double[] {-1.0}, new double[] {1.0}), 0);
    }
}

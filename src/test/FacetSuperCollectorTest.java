package test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.taxonomy.CachedOrdinalsReader;
import org.apache.lucene.facet.taxonomy.DocValuesOrdinalsReader;
import org.junit.Test;
import org.meresco.lucene.search.FacetSuperCollector;

public class FacetSuperCollectorTest extends SeecrTestCase {
    
    @Test
    public void testEmpty() {
        FacetSuperCollector f = new FacetSuperCollector(null, new FacetsConfig(), new CachedOrdinalsReader(new DocValuesOrdinalsReader()));
        assertEquals(null, f.getFirstArray());
    }
    
    @Test
    public void testOneArray() {
        FacetSuperCollector f = new FacetSuperCollector(null, new FacetsConfig(), new CachedOrdinalsReader(new DocValuesOrdinalsReader()));
        f.mergePool(new int[] {0, 1, 2, 3, 4});
        assertArrayEquals(new int[] {0, 1, 2, 3, 4}, f.getFirstArray());
    }
    
    @Test
    public void testMergeTwoArray() {
        FacetSuperCollector f = new FacetSuperCollector(null, new FacetsConfig(), new CachedOrdinalsReader(new DocValuesOrdinalsReader()));
        f.mergePool(new int[] {0, 1, 2, 3, 4});
        f.mergePool(new int[] {0, 0, 1, 1, 1});
        assertArrayEquals(new int[] {0, 1, 3, 4, 5}, f.getFirstArray());
    }
}

/* begin license *
 *
 * "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
 *
 * Copyright (C) 2015 Koninklijke Bibliotheek (KB) http://www.kb.nl
 * Copyright (C) 2015 Seecr (Seek You Too B.V.) https://seecr.nl
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

package org.meresco.lucene;

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

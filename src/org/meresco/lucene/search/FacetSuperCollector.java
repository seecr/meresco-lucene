/* begin license *
 *
 * "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
 *
 * Copyright (C) 2014 Seecr (Seek You Too B.V.) http://seecr.nl
 * Copyright (C) 2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
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
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.taxonomy.OrdinalsReader;
import org.apache.lucene.facet.taxonomy.TaxonomyFacetCounts;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;

public class FacetSuperCollector extends SuperCollector<FacetSubCollector> {

    final TaxonomyReader taxoReader;
    final FacetsConfig facetConfig;
    final OrdinalsReader ordinalsReader;
    final BlockingDeque<int[]> arrayPool = new LinkedBlockingDeque<int[]>();

    public FacetSuperCollector(TaxonomyReader taxoReader, FacetsConfig facetConfig, OrdinalsReader ordinalsReader) {
        super();
        this.taxoReader = taxoReader;
        this.facetConfig = facetConfig;
        this.ordinalsReader = ordinalsReader;
        for (int i = 0; i < 4; i++)
            this.arrayPool.add(new int[taxoReader.getSize()]);
    }

    @Override
    protected FacetSubCollector createSubCollector() throws IOException {
        return new FacetSubCollector(new FacetsCollector(), this);
    }

    public FacetResult getTopChildren(int topN, String dim, String... path) throws IOException {
        int[] values = mergePool();
        return new TaxonomyFacetCounts(this.ordinalsReader, this.taxoReader, this.facetConfig, null, values)
                .getTopChildren(topN, dim, path);
    }

    private int[] mergePool() {
        // do this pairwise in threads? is it worth it?
        long t0 = System.currentTimeMillis();
        int[] values = this.arrayPool.poll();
        while (this.arrayPool.peek() != null) {
            int[] values1 = this.arrayPool.poll();
            for (int i = 0; i < values.length; i++)
                values[i] += values1[i];
        }
        long t1 = System.currentTimeMillis();
        System.out.println("merge took (ms): " + (t1-t0));
        return values;
    }
}

class FacetSubCollector extends DelegatingSubCollector<FacetsCollector, FacetSuperCollector> {

    public FacetSubCollector(FacetsCollector delegate, FacetSuperCollector parent) throws IOException {
        super(delegate, parent);
    }

    @Override
    public void complete() throws IOException {
        int[] values;
        try {
            values = this.parent.arrayPool.take();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        TaxonomyFacetCounts counts = new TaxonomyFacetCounts(this.parent.ordinalsReader, this.parent.taxoReader,
                this.parent.facetConfig, this.delegate, values);
        counts.doCount();
        this.parent.arrayPool.push(values);
    }
}

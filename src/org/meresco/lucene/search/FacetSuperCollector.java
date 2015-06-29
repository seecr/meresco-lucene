/* begin license *
 *
 * "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
 *
 * Copyright (C) 2014-2015 Seecr (Seek You Too B.V.) http://seecr.nl
 * Copyright (C) 2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
 * Copyright (C) 2015 Koninklijke Bibliotheek (KB) http://www.kb.nl
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
import java.util.List;
import java.util.ArrayList;

import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.taxonomy.OrdinalsReader;
import org.apache.lucene.facet.taxonomy.TaxonomyFacetCounts;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;

public class FacetSuperCollector extends SuperCollector<FacetSubCollector> {

    final TaxonomyReader taxoReader;
    final FacetsConfig facetConfig;
    final List<OrdinalsReader> ordinalsReaders;
    final BlockingDeque<int[]> arrayPool = new LinkedBlockingDeque<int[]>();
    private int[] mergeValues;

    public FacetSuperCollector(TaxonomyReader taxoReader, FacetsConfig facetConfig, OrdinalsReader ordinalsReader) {
        super();
        this.taxoReader = taxoReader;
        this.facetConfig = facetConfig;
        this.ordinalsReaders = new ArrayList();
        this.ordinalsReaders.add(ordinalsReader);
    }
    public void addOrdinalsReader(OrdinalsReader ordinalsReader) {
        this.ordinalsReaders.add(ordinalsReader);
    }

    @Override
    protected FacetSubCollector createSubCollector() throws IOException {
        return new FacetSubCollector(new FacetsCollector(), this);
    }

    public FacetResult getTopChildren(int topN, String dim, String... path) throws IOException {
        // This will not really need a ordinalsReader. but will call getIndexFieldName()
        return new TaxonomyFacetCounts(this.ordinalsReaders.get(0), this.taxoReader, this.facetConfig, null, this.mergeValues){
            protected FacetsConfig.DimConfig verifyDim(String dim) {
                return config.getDimConfig(dim);
            }
        }.getTopChildren(topN, dim, path);
    }

    @Override
    public void complete() {
        this.mergeValues = this.arrayPool.poll();
        mergePool(this.mergeValues);
    }

    public void mergePool(int[] values) {
        int count = 0;
        int[] values1 = this.arrayPool.poll();
        while (values1 != null) {
            count++;
            for (int i = 0; i < values.length; i++)
                values[i] += values1[i];
            values1 = this.arrayPool.poll();
            if (count > 10000) {
                System.out.println("More than 10000 tries in FacetSuperCollector.mergePool.");
                System.out.flush();
                throw new RuntimeException("More than 10000 tries in FacetSuperCollector.mergePool.");
            }
        }
        this.arrayPool.push(values);
    }

    public int[] getFirstArray() {
        return (int[]) this.arrayPool.peek();
    }
}

class FacetSubCollector extends DelegatingSubCollector<FacetsCollector, FacetSuperCollector> {

    public FacetSubCollector(FacetsCollector delegate, FacetSuperCollector parent) throws IOException {
        super(delegate, parent);
    }

    @Override
    public void complete() throws IOException {
        int[] values = new int[this.parent.taxoReader.getSize()];
        TaxonomyFacetCounts counts = null;
        for (OrdinalsReader ordinalsReader: this.parent.ordinalsReaders) {
            System.out.println("IndexFieldName "+ordinalsReader.getIndexFieldName());
            counts = new TaxonomyFacetCounts(ordinalsReader, this.parent.taxoReader,
                    this.parent.facetConfig, this.delegate, values);
            counts.doCount(false);
        }
        counts.doRollup();
        this.parent.mergePool(values);
    }

}

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
import org.apache.lucene.facet.LabelAndValue;
import org.apache.lucene.facet.TopOrdAndIntQueue;
import org.apache.lucene.facet.taxonomy.FacetLabel;
import org.apache.lucene.facet.taxonomy.OrdinalsReader;
import org.apache.lucene.facet.taxonomy.ParallelTaxonomyArrays;
import org.apache.lucene.facet.taxonomy.TaxonomyFacetCounts;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;

public class FacetSuperCollector extends SuperCollector<FacetSubCollector> {

    final TaxonomyReader       taxoReader;
    final FacetsConfig         facetConfig;
    final OrdinalsReader       ordinalsReader;
    final BlockingDeque<int[]> valuesStack = new LinkedBlockingDeque<int[]>();

    public FacetSuperCollector(TaxonomyReader taxoReader, FacetsConfig facetConfig, OrdinalsReader ordinalsReader) {
        super();
        this.taxoReader = taxoReader;
        this.facetConfig = facetConfig;
        this.ordinalsReader = ordinalsReader;
        for (int i = 0; i < 10; i++)
            this.valuesStack.add(new int[taxoReader.getSize()]);
    }

    @Override
    protected FacetSubCollector createSubCollector() throws IOException {
        return new FacetSubCollector(new FacetsCollector(), this);
    }

    public FacetResult getTopChildren(int topN, String dim, String... path) throws IOException {
        if (topN <= 0) {
            throw new IllegalArgumentException("topN must be > 0 (got: " + topN + ")");
        }
        FacetsConfig.DimConfig dimConfig = this.facetConfig.getDimConfig(dim);
        // if (!dimConfig.indexFieldName.equals(indexFieldName)) {
        // throw new IllegalArgumentException("dimension \"" + dim +
        // "\" was not indexed into field \"" + indexFieldName);
        // }

        FacetLabel cp = new FacetLabel(dim, path);
        int dimOrd = this.taxoReader.getOrdinal(cp);
        if (dimOrd == -1) {
            return null;
        }

        TopOrdAndIntQueue q = new TopOrdAndIntQueue(Math.min(taxoReader.getSize(), topN));

        int totValue = 0;
        int childCount = 0;

        ParallelTaxonomyArrays pta = this.taxoReader.getParallelTaxonomyArrays();
        int[] children = pta.children();
        int[] siblings = pta.siblings();

        int ord = children[dimOrd];
        TopOrdAndIntQueue.OrdAndValue reuse = null;
        int bottomValue = 0;

        while (ord != TaxonomyReader.INVALID_ORDINAL) {
            int val = 0;
            for (int[] values: this.valuesStack)
                val += values[ord];
            //for (FacetSubCollector sub : super.subs) {
            //    val += sub.values[ord];
            //}
            if (val > 0) {
                totValue += val;
                childCount++;
                if (val > bottomValue) {
                    if (reuse == null) {
                        reuse = new TopOrdAndIntQueue.OrdAndValue();
                    }
                    reuse.ord = ord;
                    reuse.value = val;
                    reuse = q.insertWithOverflow(reuse);
                    if (q.size() == topN) {
                        bottomValue = q.top().value;
                    }
                }
            }
            ord = siblings[ord];
        }

        if (totValue == 0) {
            return null;
        }

        if (dimConfig.multiValued) {
            if (dimConfig.requireDimCount) {
                totValue = 0;
                for (int[] values: this.valuesStack)
                    totValue += values[dimOrd];
                //for (FacetSubCollector sub : super.subs) {
                //    totValue += sub.values[dimOrd];
                //}
            } else {
                // Our sum'd value is not correct, in general:
                totValue = -1;
            }
        } else {
            // Our sum'd dim value is accurate, so we keep it
        }

        LabelAndValue[] labelValues = new LabelAndValue[q.size()];
        for (int i = labelValues.length - 1; i >= 0; i--) {
            TopOrdAndIntQueue.OrdAndValue ordAndValue = q.pop();
            FacetLabel child = this.taxoReader.getPath(ordAndValue.ord);
            labelValues[i] = new LabelAndValue(child.components[cp.length], ordAndValue.value);
        }

        return new FacetResult(dim, path, totValue, labelValues, childCount);
    }
}

class FacetSubCollector extends DelegatingSubCollector<FacetsCollector, FacetSuperCollector> {

    int[] values;

    public FacetSubCollector(FacetsCollector delegate, FacetSuperCollector parent) throws IOException {
        super(delegate, parent);
    }

    @Override
    public void complete() throws IOException {
        try {
            this.values = this.parent.valuesStack.take();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        SubTaxonomyFacetCounts counts = new SubTaxonomyFacetCounts(this.parent.ordinalsReader, this.parent.taxoReader,
                this.parent.facetConfig, this.delegate, this.values);
        counts.getValues();
    }
}

class SubTaxonomyFacetCounts extends TaxonomyFacetCounts {
    public SubTaxonomyFacetCounts(OrdinalsReader ordinalsReader, TaxonomyReader taxoReader, FacetsConfig config,
            FacetsCollector fc, int[] values) throws IOException {
        super(ordinalsReader, taxoReader, config, fc, values);
    }

    public int[] getValues() throws IOException {
        this.doCount();
        return this.values;
    }
}

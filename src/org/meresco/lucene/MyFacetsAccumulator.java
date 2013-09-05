/* begin license *
 *
 * "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
 *
 * Copyright (C) 2013 Seecr (Seek You Too B.V.) http://seecr.nl
 * Copyright (C) 2013 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
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

import org.apache.lucene.facet.search.FacetsAggregator;
import org.apache.lucene.facet.search.FacetsAccumulator;
import org.apache.lucene.facet.params.FacetSearchParams;
import org.apache.lucene.facet.search.FacetsAggregator;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.search.FacetArrays;

public class MyFacetsAccumulator extends FacetsAccumulator {

    private FacetsAggregator aggregator;

    public MyFacetsAccumulator(FacetsAggregator aggregator, FacetSearchParams searchParams, IndexReader indexReader, TaxonomyReader taxonomyReader, FacetArrays facetArrays)
    {
        super(searchParams, indexReader, taxonomyReader, facetArrays);
        this.aggregator = aggregator;
    }

    @Override
    public FacetsAggregator getAggregator() {
        return this.aggregator;
    }
}
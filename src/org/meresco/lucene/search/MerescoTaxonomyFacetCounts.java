/* begin license *
 *
 * "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
 *
 * Copyright (C) 2015 Koninklijke Bibliotheek (KB) http://www.kb.nl
 * Copyright (C) 2015 Seecr (Seek You Too B.V.) http://seecr.nl
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
import java.util.List;

import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsCollector.MatchingDocs;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.taxonomy.IntTaxonomyFacets;
import org.apache.lucene.facet.taxonomy.OrdinalsReader;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.IntsRef;

public class MerescoTaxonomyFacetCounts extends IntTaxonomyFacets {

    private final List<OrdinalsReader> ordinalsReaders;
    private FacetsCollector fc;

    public MerescoTaxonomyFacetCounts(List<OrdinalsReader> ordinalsReaders, TaxonomyReader taxoReader, FacetsConfig config,
            FacetsCollector fc, int[] valuesArray) throws IOException {
        super(ordinalsReaders.get(0).getIndexFieldName(), taxoReader, config, valuesArray);
        this.ordinalsReaders = ordinalsReaders;
        this.fc = fc;
    }

    final void doCount() throws IOException {
        count(this.fc.getMatchingDocs());
    }

    private final void count(List<MatchingDocs> matchingDocs) throws IOException {
        IntsRef scratch = new IntsRef();
        OrdinalsReader.OrdinalsSegmentReader[] ordsReaders = new OrdinalsReader.OrdinalsSegmentReader[this.ordinalsReaders.size()];
        for (MatchingDocs hits : matchingDocs) {
            for (int i = 0; i < ordsReaders.length; i++) {
                ordsReaders[i] = this.ordinalsReaders.get(i).getReader(hits.context);
            }
            DocIdSetIterator docs = hits.bits.iterator();
            int doc;
            while ((doc = docs.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                for (OrdinalsReader.OrdinalsSegmentReader ords : ordsReaders) {
                    ords.get(doc, scratch);
                    for (int i = 0; i < scratch.length; i++) {
                        values[scratch.ints[scratch.offset + i]]++;
                    }
                }
            }
        }

        rollup();
    }

    protected FacetsConfig.DimConfig verifyDim(String dim) {
        return this.config.getDimConfig(dim);
    }
}

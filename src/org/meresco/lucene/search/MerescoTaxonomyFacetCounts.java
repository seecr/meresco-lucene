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
import java.util.Map;

import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsCollector.MatchingDocs;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.FacetsConfig.DimConfig;
import org.apache.lucene.facet.LabelAndValue;
import org.apache.lucene.facet.TopOrdAndIntQueue;
import org.apache.lucene.facet.taxonomy.FacetLabel;
import org.apache.lucene.facet.taxonomy.OrdinalsReader;
import org.apache.lucene.facet.taxonomy.TaxonomyFacets;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.IntsRef;


public class MerescoTaxonomyFacetCounts extends TaxonomyFacets {
    private final List<OrdinalsReader> ordinalsReaders;
    private FacetsCollector fc;
    private int[] values;

    public MerescoTaxonomyFacetCounts(List<OrdinalsReader> ordinalsReaders, TaxonomyReader taxoReader, FacetsConfig config,
            FacetsCollector fc, int[] valuesArray) throws IOException {
        super(ordinalsReaders.get(0).getIndexFieldName(), taxoReader, config);
        this.values = valuesArray;
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

    @Override
    protected FacetsConfig.DimConfig verifyDim(String dim) {
        return this.config.getDimConfig(dim);
    }


    /** Rolls up any single-valued hierarchical dimensions. */
    protected void rollup() throws IOException {
      int[] children = getChildren();
      // Rollup any necessary dims:
      for (Map.Entry<String,DimConfig> ent : config.getDimConfigs().entrySet()) {
        String dim = ent.getKey();
        DimConfig ft = ent.getValue();
        if (ft.hierarchical && ft.multiValued == false) {
          int dimRootOrd = taxoReader.getOrdinal(new FacetLabel(dim));
          // It can be -1 if this field was declared in the
          // config but never indexed:
          if (dimRootOrd > 0) {
            values[dimRootOrd] += rollup(children[dimRootOrd]);
          }
        }
      }
    }

    private int rollup(int ord) {
        try {
            int[] children = getChildren();
            int[] siblings = getSiblings();
            int sum = 0;
            while (ord != TaxonomyReader.INVALID_ORDINAL) {
                int childValue = values[ord] + rollup(children[ord]);
                values[ord] = childValue;
                sum += childValue;
                ord = siblings[ord];
            }
            return sum;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Number getSpecificValue(String dim, String... path) throws IOException {
      DimConfig dimConfig = verifyDim(dim);
      if (path.length == 0) {
        if (dimConfig.hierarchical && dimConfig.multiValued == false) {
          // ok: rolled up at search time
        } else if (dimConfig.requireDimCount && dimConfig.multiValued) {
          // ok: we indexed all ords at index time
        } else {
          throw new IllegalArgumentException("cannot return dimension-level value alone; use getTopChildren instead");
        }
      }
      int ord = taxoReader.getOrdinal(new FacetLabel(dim, path));
      if (ord < 0) {
        return -1;
      }
      return values[ord];
    }

    @Override
    public FacetResult getTopChildren(int topN, String dim, String... path) throws IOException {
      if (topN <= 0) {
        throw new IllegalArgumentException("topN must be > 0 (got: " + topN + ")");
      }
      DimConfig dimConfig = verifyDim(dim);
      FacetLabel cp = new FacetLabel(dim, path);
      int dimOrd = taxoReader.getOrdinal(cp);
      if (dimOrd == -1) {
        return null;
      }

      TopOrdAndIntQueue q = new TopOrdAndIntQueue(Math.min(taxoReader.getSize(), topN));

      int bottomValue = 0;

      int ord = getChildren()[dimOrd];
      int[] siblings = getSiblings();

      int totValue = 0;
      int childCount = 0;

      TopOrdAndIntQueue.OrdAndValue reuse = null;
      while (ord != TaxonomyReader.INVALID_ORDINAL) {
        if (values[ord] > 0) {
          totValue += values[ord];
          childCount++;
          if (values[ord] > bottomValue) {
            if (reuse == null) {
              reuse = new TopOrdAndIntQueue.OrdAndValue();
            }
            reuse.ord = ord;
            reuse.value = values[ord];
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
          totValue = values[dimOrd];
        } else {
          // Our sum'd value is not correct, in general:
          totValue = -1;
        }
      } else {
        // Our sum'd dim value is accurate, so we keep it
      }

      LabelAndValue[] labelValues = new LabelAndValue[q.size()];
      for(int i=labelValues.length-1;i>=0;i--) {
        TopOrdAndIntQueue.OrdAndValue ordAndValue = q.pop();
        FacetLabel child = taxoReader.getPath(ordAndValue.ord);
        labelValues[i] = new LabelAndValue(child.components[cp.length], ordAndValue.value);
      }

      return new FacetResult(dim, path, totValue, labelValues, childCount);
    }
}

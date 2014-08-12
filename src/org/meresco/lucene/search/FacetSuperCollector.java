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

import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.taxonomy.CachedOrdinalsReader;
import org.apache.lucene.facet.taxonomy.DocValuesOrdinalsReader;
import org.apache.lucene.facet.taxonomy.TaxonomyFacetCounts;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.facet.LabelAndValue;

import java.util.Map;
import java.util.HashMap;
import java.lang.Number;

public class FacetSuperCollector extends SuperCollector<FacetSubCollector> {

	final TaxonomyReader taxoReader;
	final FacetsConfig facetConfig;
	final String dim;
	final int topN;
	final String[] path;

	public FacetSuperCollector(TaxonomyReader taxoReader, FacetsConfig facetConfig, String dim, int topN, String... path) {
		super();
		this.taxoReader = taxoReader;
		this.facetConfig = facetConfig;
		this.dim = dim;
		this.topN = topN;
		this.path = path;
	}

	@Override
	protected FacetSubCollector createSubCollector(AtomicReaderContext context) throws IOException {
		return new FacetSubCollector(context, new FacetsCollector(), this);
	}

	public FacetResult getTopChildren() throws IOException {
		Map<String, Integer> labelValues = new HashMap<String, Integer>();
		for(FacetSubCollector sub : super.subs) {
			FacetResult subResults = sub.results;
			for (LabelAndValue label : subResults.labelValues) {
				Integer currentValue = labelValues.get(label.label);
				if (currentValue == null) {
					labelValues.put(label.label, label.value.intValue());
				} else {
					labelValues.put(label.label, label.value.intValue() + currentValue);
				}
			}
		}
		TopStringAndIntQueue lv = new TopStringAndIntQueue(this.topN);
		for (Map.Entry<String, Integer> entry : labelValues.entrySet()) {
			TopStringAndIntQueue.StringAndValue sv = new TopStringAndIntQueue.StringAndValue(entry.getKey(), entry.getValue());
			lv.insertWithOverflow(sv);
		}

		LabelAndValue[] labelAndValues = new LabelAndValue[this.topN];
		while (lv.size() > 0) {
			TopStringAndIntQueue.StringAndValue sv = lv.pop();
			labelAndValues[lv.size()] = new LabelAndValue(sv.ord, sv.value);
		}
		return new FacetResult(null, null, null, labelAndValues, 0);
	}
}

class FacetSubCollector extends DelegatingSubCollector<FacetsCollector, FacetSuperCollector> {

	FacetResult results;

	public FacetSubCollector(AtomicReaderContext context, FacetsCollector delegate, FacetSuperCollector parent)
			throws IOException {
		super(context, delegate, parent);
	}

	@Override
	public void complete() throws IOException {
		TaxonomyFacetCounts counts = new TaxonomyFacetCounts(new CachedOrdinalsReader(
				new DocValuesOrdinalsReader()), this.parent.taxoReader, this.parent.facetConfig,
				(FacetsCollector) this.delegate);
		this.results = counts.getTopChildren(Integer.MAX_VALUE, this.parent.dim, this.parent.path);
	}
}

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
import org.apache.lucene.search.Collector;

public class FacetSuperCollector extends SuperCollector<FacetSubCollector> {

	final TaxonomyReader taxoReader;
	final FacetsConfig facetConfig;
	final String dim;
	final int topN;
	final String path;

	public FacetSuperCollector(TaxonomyReader taxoReader, FacetsConfig facetConfig, String dim, int topN, String path) {
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

	public FacetResult getTopChildren(int topN, String dim, String... path) throws IOException {
		return null;
	}
}

class FacetSubCollector extends DelegatingSubCollector {

	public FacetSubCollector(AtomicReaderContext context, Collector delegate, SuperCollector parent) throws IOException {
		super(context, delegate, parent);
	}

	@Override
	public void complete() {
		// TaxonomyFacetCounts(self._ordinalsReader,
		// self._indexAndTaxonomy.taxoReader, self._facetsConfig,
		// facetCollector)
		TaxonomyFacetCounts facetResult = new TaxonomyFacetCounts(new CachedOrdinalsReader(
				new DocValuesOrdinalsReader()), this.parent.taxoReader, this.parent.facetConfig,
				(FacetsCollector) this.delegate);
		facetResult.getTopChildren(this.parent.topN, this.parent.dim, this.path);
	}
}

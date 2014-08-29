package org.meresco.lucene.search;

import java.io.IOException;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Scorer;

public abstract class DelegatingSubCollector<CollectorType extends Collector, SuperCollectorType extends SuperCollector<?>>
		extends SubCollector {

	protected CollectorType delegate;
	protected SuperCollectorType parent;

	public DelegatingSubCollector(CollectorType delegate, SuperCollectorType parent) throws IOException {
		super();
		this.delegate = delegate;
		this.parent = parent;
	}

	@Override
	public void setScorer(Scorer scorer) throws IOException {
		this.delegate.setScorer(scorer);
	}

	@Override
	public void collect(int doc) throws IOException {
		this.delegate.collect(doc);
	}

	@Override
	public boolean acceptsDocsOutOfOrder() {
		return this.delegate.acceptsDocsOutOfOrder();
	}

	@Override
	public void setNextReader(AtomicReaderContext context) throws IOException {
		//System.out.println("Setting: " + context);
		this.delegate.setNextReader(context);
	}
}

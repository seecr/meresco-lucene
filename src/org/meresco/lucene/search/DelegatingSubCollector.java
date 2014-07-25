package org.meresco.lucene.search;

import java.io.IOException;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Scorer;

public abstract class DelegatingSubCollector<CollectorType extends Collector> extends SubCollector {

	protected CollectorType delegate;

	public DelegatingSubCollector(AtomicReaderContext context, CollectorType delegate) throws IOException {
		super(context);
		this.delegate = delegate;
		if (context != null)
			this.delegate.setNextReader(context);
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
}

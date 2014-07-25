package org.meresco.lucene.search;

import java.io.IOException;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Scorer;

public class CountingSuperCollector extends SuperCollector<CountingSubCollector> {

	@Override
	public CountingSubCollector createSubCollector(AtomicReaderContext context) {
		return new CountingSubCollector(context);
	}

	public int count() {
		int n = 0;
		for (CountingSubCollector sub : super.subs) {
			n += sub.count;
		}
		return n;
	}

}

class CountingSubCollector extends SubCollector {

	public CountingSubCollector(AtomicReaderContext context) {
		super(context);
	}

	int count = 0;
	
	@Override
	public void setScorer(Scorer scorer) throws IOException {
	}

	@Override
	public void collect(int doc) throws IOException {
		this.count++;
	}

	@Override
	public boolean acceptsDocsOutOfOrder() {
		return true;
	}

	@Override
	public void complete() {
	}
}
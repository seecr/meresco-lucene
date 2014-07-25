package org.meresco.lucene.search;

import java.io.IOException;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopScoreDocCollector;

public class TopDocsSuperCollector extends SuperCollector<TopDocsSubCollector> {

	private int numHits;
	private boolean docsScoredInOrder;

	public TopDocsSuperCollector(int numHits, boolean docsScoredInOrder) {
		super();
		this.numHits = numHits;
		this.docsScoredInOrder = docsScoredInOrder;
	}

	@Override
	protected TopDocsSubCollector createSubCollector(AtomicReaderContext context) throws IOException {
		return new TopDocsSubCollector(context, this.numHits, this.docsScoredInOrder);
	}

	public TopDocs topDocs() throws IOException {
		TopDocs[] topdocs = new TopDocs[super.subs.size()];
		for (int i = 0; i < topdocs.length; i++)
			topdocs[i] = super.subs.get(i).topdocs;
		return TopDocs.merge(null, this.numHits, topdocs);
	}
}

class TopDocsSubCollector extends DelegatingSubCollector<TopScoreDocCollector> {

	TopDocs topdocs;

	public TopDocsSubCollector(AtomicReaderContext context, int numHits, boolean docsScoredInOrder) throws IOException {
		super(context, TopScoreDocCollector.create(numHits, docsScoredInOrder));
	}

	@Override
	public void complete() {
		this.topdocs = this.delegate.topDocs();
	}
}

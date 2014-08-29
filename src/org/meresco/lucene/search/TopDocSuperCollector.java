package org.meresco.lucene.search;

import java.io.IOException;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;

public abstract class TopDocSuperCollector extends SuperCollector<TopDocSubCollector<?>> {

	protected final Sort sort;
	protected final int numHits;

	public TopDocSuperCollector(Sort sort, int numHits) {
		super();
		this.sort = sort;
		this.numHits = numHits;
	}

	public TopDocs topDocs(int start) throws IOException {
		TopDocs[] topdocs = new TopDocs[this.subs.size()];
		for (int i = 0; i < topdocs.length; i++) {
			topdocs[i] = this.subs.get(i).topdocs;
			for (ScoreDoc d : topdocs[i].scoreDocs) {
				System.out.println(d);
			}
		}
		return TopDocs.merge(this.sort, start, this.numHits - start, topdocs);
	}

	public int getTotalHits() throws IOException {
		return this.topDocs(0).totalHits;
	}
}
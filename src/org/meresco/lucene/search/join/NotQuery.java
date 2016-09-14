package org.meresco.lucene.search.join;

import org.apache.lucene.util.BitSet;


public class NotQuery implements RelationalQuery {
	private RelationalQuery q;
	private BitSet keyFilter;
	private boolean inverted = false;

	public NotQuery(RelationalQuery q) {
		this.q = q;
	}

	@Override
	public Result execute() {
		if (!this.inverted) {
			this.q.invert();
		}
		if (this.keyFilter != null) {
			this.q.addFilter(this.keyFilter);
		}
		return this.q.execute();
	}

	@Override
	public void addFilter(BitSet keyFilter) {
    	this.keyFilter = keyFilter;
	}

	@Override
	public void invert() {
		this.inverted = !this.inverted;
	}
}

package org.meresco.lucene.search.join;


public class NotQuery implements RelationalQuery {
	private RelationalQuery q;
	private IntermediateResult keyFilter;
	private boolean inverted = false;

	public NotQuery(RelationalQuery q) {
		this.q = q;
	}

	@Override
	public IntermediateResult execute() {
		if (!this.inverted) {
			this.q.invert();
		}
		if (this.keyFilter != null) {
			this.q.addFilter(this.keyFilter);
		}
		return this.q.execute();
	}

	@Override
	public void addFilter(IntermediateResult keyFilter) {
    	this.keyFilter = keyFilter;
	}

	@Override
	public void invert() {
		this.inverted = !this.inverted;
	}
}

package org.meresco.lucene.search.join;

public interface RelationalQuery {
    public IntermediateResult execute();

    public void addFilter(IntermediateResult keyFilter);

	public void invert();
}

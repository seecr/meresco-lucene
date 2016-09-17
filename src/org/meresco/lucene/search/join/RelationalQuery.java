package org.meresco.lucene.search.join;

public interface RelationalQuery {
    public IntermediateResult execute();

    public void filter(IntermediateResult keyFilter);

	public void invert();

	public void union(IntermediateResult intermediateResult);
}

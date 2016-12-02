package org.meresco.lucene.search.join.relational;

import java.util.Map;

import org.meresco.lucene.Lucene;


public interface RelationalQuery {
    public IntermediateResult collectKeys(Map<String, Lucene> lucenes);

    public void filter(IntermediateResult keyFilter);

	public void union(IntermediateResult intermediateResult);

	public void invert();
}

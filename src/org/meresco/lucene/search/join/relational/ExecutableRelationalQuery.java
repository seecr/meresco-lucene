package org.meresco.lucene.search.join.relational;


public interface ExecutableRelationalQuery extends RelationalQuery {
    public void filter(IntermediateResult keyFilter);

    public void union(IntermediateResult intermediateResult);

    public void invert();
}

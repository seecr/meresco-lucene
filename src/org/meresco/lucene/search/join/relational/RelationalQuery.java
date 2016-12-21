package org.meresco.lucene.search.join.relational;

import java.util.Map;

import org.meresco.lucene.Lucene;


public interface RelationalQuery {
    public IntermediateResult collectKeys(Map<String, Lucene> lucenes);

    public ExecutableRelationalQuery asExecutable();
}

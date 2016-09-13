package org.meresco.lucene.search.join;

import org.apache.lucene.util.BitSet;

public interface RelationalQuery {
    public Result execute(Operation op);

    public void addFilter(BitSet keyFilter);
}

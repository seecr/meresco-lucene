package org.meresco.lucene.search.join;

import org.apache.lucene.util.BitSet;

public interface RelationalQuery {
    public Result execute();

    public void addFilter(BitSet keyFilter, String keyName);

    void prepareCollectKeys(String keyName);

}

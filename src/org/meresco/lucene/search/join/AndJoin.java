package org.meresco.lucene.search.join;

import java.io.IOException;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BitSet;
import org.meresco.lucene.queries.KeyFilter;


public class AndJoin implements Operation {
    private BitSet keyFilter;

    public AndJoin(Result result) {
        this.keyFilter = result.getBitSet();
    }

    public void rewrite(RelationalQuery q) {
        q.addFilter(keyFilter);
    }
}

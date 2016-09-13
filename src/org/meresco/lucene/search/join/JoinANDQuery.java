package org.meresco.lucene.search.join;

import org.apache.lucene.util.BitSet;

public class JoinANDQuery implements RelationalQuery {
    private RelationalQuery second;
    private RelationalQuery first;

    public JoinANDQuery(RelationalQuery first, RelationalQuery second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public Result execute(Operation op) {
        Result result = this.first.execute(op);
        Operation andJoin = new AndJoin(result);
        return this.second.execute(andJoin);
    }

    @Override
    public void addFilter(BitSet keyFilter) {
        // TODO Auto-generated method stub
        
    }
}
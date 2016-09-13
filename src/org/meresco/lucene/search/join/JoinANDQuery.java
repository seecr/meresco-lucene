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
    public Result execute() {
        Result result = this.first.execute();
        BitSet bits = result.getBitSet();
        System.out.println("bits: " + bits);
        this.second.addFilter(bits);
        return this.second.execute();
    }

    @Override
    public void addFilter(BitSet keyFilter) {
        // TODO Auto-generated method stub
    }
}
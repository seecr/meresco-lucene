package org.meresco.lucene.search.join;

import org.apache.lucene.util.BitSet;


public class JoinANDQuery implements RelationalQuery {
    private RelationalQuery second;
    private RelationalQuery first;
	private BitSet keyFilter;

    public JoinANDQuery(RelationalQuery first, RelationalQuery second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public Result execute() {
    	if (this.keyFilter != null) {
    		this.first.addFilter(this.keyFilter);
    	}
        Result result = this.first.execute();
        BitSet bits = result.getBitSet();
        this.second.addFilter(bits);
        return this.second.execute();
    }

    @Override
    public void addFilter(BitSet keyFilter) {
    	this.keyFilter = keyFilter;
    }
}
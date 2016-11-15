package org.meresco.lucene.search.join;


public class JoinANDQuery implements RelationalQuery {
    private RelationalQuery first;
    private RelationalQuery second;
	private IntermediateResult filter;
	private IntermediateResult union;
	private boolean inverted;

    public JoinANDQuery(RelationalQuery first, RelationalQuery second) {
        this.first = first;
        this.second = second;
    }

    @Override
	public String toString() {
    	return "JoinANDQuery(" + first + ", " + second + ")";
    }

    @Override
    public IntermediateResult execute() {
    	System.out.println("execute " + this);
    	if (this.filter != null) {
			System.out.println("apply filter " + this.filter + " to ANDQuery.first " + this.first);
    		this.first.filter(this.filter);
    	}
        IntermediateResult result = this.first.execute();
		System.out.println("apply filter " + result + " to ANDQuery.second " + this.second);
        this.second.filter(result);
    	if (!this.inverted && this.union != null) {
			System.out.println("apply union " + this.union + " to ANDQuery.second " + this.second);
    		this.second.union(this.union);
    	}
        result = this.second.execute();

        if (this.inverted) {
        	result.inverted = true;
        	if (this.filter != null) {
        		System.out.println("[JoinANDQuery] applying bitset filter " + result + " to filter " + this.filter);
        		this.filter.intersect(result);  // note: no ranking (but shouldn't be an issue)
        		return this.filter;  // TODO: !! what if there's both a filter and a union (from higher up?)
        	}
        	else if (this.union != null) {
        		System.out.println("[JoinANQuery] applying bitset union " + result + " to union " + this.union);
        		this.union.union(result);  // note: no ranking (but shouldn't be an issue)
        		return this.union;
        	}
        }
        return result;
    }

	@Override
	public void invert() {
		this.inverted = true;
	}

    @Override
    public void filter(IntermediateResult intermediateResult) {
    	assert this.filter == null;
    	this.filter = intermediateResult;
    }

	@Override
	public void union(IntermediateResult intermediateResult) {
		assert this.union == null;
		this.union = intermediateResult;
	}
}

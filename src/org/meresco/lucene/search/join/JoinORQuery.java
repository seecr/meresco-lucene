package org.meresco.lucene.search.join;


public class JoinORQuery implements RelationalQuery {
    private RelationalQuery first;
    private RelationalQuery second;
	private IntermediateResult filter;
	private IntermediateResult union;
	private boolean inverted;

    public JoinORQuery(RelationalQuery first, RelationalQuery second) {
        this.first = first;
        this.second = second;
    }

    @Override
	public String toString() {
    	return "JoinORQuery(" + first + ", " + second + ")";
    }

    @Override
    public IntermediateResult execute() {
    	System.out.println("execute " + this);
    	if (this.filter != null) {
			System.out.println("apply filter " + this.filter + " to ORQuery.first " + this.first);
    		this.first.filter(this.filter);
    	}
    	else if (this.union != null && !this.inverted) {
			System.out.println("apply union " + this.union + " to ORQuery.first " + this.first);
    		this.first.union(this.union);
    	}
        IntermediateResult resultFirst = this.first.execute();

    	if (this.filter != null) {
			System.out.println("apply filter " + filter + " to ORQuery.second " + this.second);
    		this.second.filter(this.filter);
    	}
		System.out.println("apply union " + resultFirst + " to ORQuery.second " + this.second);
    	this.second.union(resultFirst);
        IntermediateResult result = this.second.execute();

        if (this.inverted) {
        	result.inverted = true;
        	if (this.filter != null) {
        		System.out.println("[JoinORQuery] applying bitset filter " + result + " to filter " + this.filter);
        		this.filter.intersect(result);
        		System.out.println("result: " + this.filter);
        		return this.filter;  // TODO: !! what if there's both a filter and a union (from higher up?)
        	}
        	else if (this.union != null) {
        		System.out.println("[JoinORQuery] applying bitset union " + result + " to union " + this.union);
	        	this.union.union(result);
        		System.out.println("result: " + this.union);
	        	return this.union;
        	}
        }
        else {
        	if (this.union != null) {
        		System.out.println("applying bitset union " + this.union + " to result " + result);
        		result.union(this.union);
        	}
        }

		System.out.println("result: " + result);
        return result;
    }

	@Override
	public void invert() {
		this.inverted = true;
	}

    @Override
    public void filter(IntermediateResult keyFilter) {
    	this.filter = keyFilter;
    }

	@Override
	public void union(IntermediateResult intermediateResult) {
		this.union = intermediateResult;
	}
}

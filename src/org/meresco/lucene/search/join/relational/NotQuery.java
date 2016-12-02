package org.meresco.lucene.search.join.relational;

import java.util.Map;

import org.meresco.lucene.Lucene;


public class NotQuery implements RelationalQuery {
	private RelationalQuery q;
	private IntermediateResult filter;
	private IntermediateResult union;
	private boolean inverted = false;

	public NotQuery(RelationalQuery q) {
		this.q = q;
	}

	@Override
	public String toString() {
		return "NotQuery(" + this.q + ")";
	}

	@Override
	public IntermediateResult collectKeys(Map<String, Lucene> lucenes) {
//    	System.out.println("execute " + this);
		if (!this.inverted) {
			this.q.invert();
		}
		if (this.filter != null) {
//			System.out.println("apply filter " + this.filter + " to NotQuery.q " + this.q);
			this.q.filter(this.filter);
		}
		else if (this.union != null) {
//			System.out.println("apply union " + this.union + " to NotQuery.q" + this.q);
			this.q.union(this.union);
		}
		IntermediateResult result = this.q.collectKeys(lucenes);
    	if (this.union != null) {
//    		System.out.println("[NotQuery] applying bitset union " + this.union + " to result " + result);
    		result.union(this.union);
//    		System.out.println("result: " + result);
    	}
		return result;
	}

	@Override
	public void invert() {
		assert !this.inverted;
		this.inverted = true;
	}

	@Override
	public void filter(IntermediateResult keyFilter) {
		assert this.filter == null;
    	this.filter = keyFilter;
	}

	@Override
	public void union(IntermediateResult intermediateResult) {
		assert this.union == null;
		this.union = intermediateResult;
	}
}

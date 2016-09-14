package org.meresco.lucene.search.join;


public class JoinANDQuery implements RelationalQuery {
    private RelationalQuery second;
    private RelationalQuery first;
	private IntermediateResult keyFilter;
	private boolean inverted;


    public JoinANDQuery(RelationalQuery first, RelationalQuery second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public IntermediateResult execute() {
    	if (this.keyFilter != null && !this.inverted) {
    		this.first.addFilter(this.keyFilter);
    	}
        IntermediateResult result = this.first.execute();
        this.second.addFilter(result);
        result = this.second.execute();
        result.inverted = this.inverted;

    	if (this.keyFilter != null && result.inverted) {
    		System.out.println("about to intersect explicitly");
    		this.keyFilter.intersect(result);  // useless for ranking...
    	}
        return result;
    }

    @Override
    public void addFilter(IntermediateResult keyFilter) {
    	this.keyFilter = keyFilter;
    }

	@Override
	public void invert() {
		this.inverted = !this.inverted;
	}
}

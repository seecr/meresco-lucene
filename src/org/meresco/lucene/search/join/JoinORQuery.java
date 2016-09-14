package org.meresco.lucene.search.join;


public class JoinORQuery implements RelationalQuery {
    private RelationalQuery second;
    private RelationalQuery first;
	private IntermediateResult keyFilter;
	private boolean inverted;

    public JoinORQuery(RelationalQuery first, RelationalQuery second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public IntermediateResult execute() {
    	throw new RuntimeException("not yet implemented");
//    	if (this.keyFilter != null) {
//    		this.first.addFilter(this.keyFilter);
//    	}
//        IntermediateResult resultFirst = this.first.execute();
//
//    	if (this.keyFilter != null) {
//    		this.second.addFilter(this.keyFilter);
//    	}
//    	this.second.addOr(resultFirst);
//        IntermediateResult result = this.second.execute();
//
//        result.inverted = this.inverted;
//        return result;
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
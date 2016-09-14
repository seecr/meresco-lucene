package org.meresco.lucene.search.join;

import org.apache.lucene.util.FixedBitSet;


public class IntermediateResult {
    private FixedBitSet bitset;
    public boolean inverted;

	public IntermediateResult(FixedBitSet bitSet) {
        this.bitset = bitSet;
    }

    public FixedBitSet getBitSet() {
        return bitset;
    }

	public void intersect(IntermediateResult other) {
		// mutates in place ...
		FixedBitSet otherBitSet = other.getBitSet();
		this.bitset = FixedBitSet.ensureCapacity(this.bitset, otherBitSet.length());
		if (this.inverted) {
			this.bitset.flip(0, this.bitset.length());  // seems expensive
		}
		if (other.inverted) {
			this.bitset.andNot(otherBitSet);
		}
		else {
			this.bitset.and(otherBitSet);
		}
		this.inverted = false;
	}
}

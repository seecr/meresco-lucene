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

	public void intersect(IntermediateResult other) {  // TODO: test!
		// mutates in place ...
		FixedBitSet otherBitSet = other.getBitSet();
		System.out.println("lenghts: " + this.bitset.length() + ", " + otherBitSet.length());
		this.bitset = FixedBitSet.ensureCapacity(this.bitset, otherBitSet.length());
		System.out.println("lenghts: " + this.bitset.length() + ", " + otherBitSet.length());
		System.out.println("before inversion: " + this.bitset.getBits());
		if (this.inverted) {
			this.bitset.flip(0, this.bitset.length());  // seems expensive
			System.out.println("after inversion: " + this.bitset.getBits());
		}
		if (other.inverted) {
			System.out.println("andNot " + otherBitSet.getBits());
			this.bitset.andNot(otherBitSet);
		}
		else {
			this.bitset.and(otherBitSet);
		}
		System.out.println("result: " + this.bitset.getBits());
		this.inverted = false;
	}
}

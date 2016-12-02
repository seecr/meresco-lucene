package org.meresco.lucene.search.join.relational;

import org.apache.lucene.search.DocIdSetIterator;
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

    /**
     * mutates in place
     */
	public void intersect(IntermediateResult other) {
		FixedBitSet otherBitSet = other.getBitSet();
		this.normalizeBitSet(otherBitSet.length());
		if (other.inverted) {
			this.bitset.andNot(otherBitSet);
		}
		else {
			this.bitset.and(otherBitSet);
		}
	}

	/**
	 * mutates in place
	 */
	public void union(IntermediateResult other) {
		other.normalizeBitSet(this.bitset.length());
		FixedBitSet otherBitSet = other.getBitSet();
		this.normalizeBitSet(otherBitSet.length());
		this.bitset.or(otherBitSet);
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("IntermediateResult(bitset=[");
		boolean listStart = true;
		if (this.bitset.cardinality() > 0) {
			for (int i = this.bitset.nextSetBit(0); i != DocIdSetIterator.NO_MORE_DOCS; i = this.bitset.nextSetBit(i+1)) {
				if (!listStart) {
					sb.append(", ");
				}
				sb.append("" + i);
				listStart = false;
			}
		}
		sb.append("], inverted=" + inverted + ")");
		return sb.toString();
	}

	public IntermediateResult normalizeBitSet(int size) {
		this.bitset = FixedBitSet.ensureCapacity(this.bitset, size);
		if (this.inverted) {
			this.bitset.flip(0, this.bitset.length());  // seems expensive
			this.inverted = false;
		}
		return this;
	}
}

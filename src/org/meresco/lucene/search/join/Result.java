package org.meresco.lucene.search.join;

import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.FixedBitSet;


public class Result {
    private BitSet bitset;

	public Result(FixedBitSet collectedKeys) {
        this.bitset = collectedKeys;
    }

    public Result intersect(Result resultRhs) {
        // TODO Auto-generated method stub
        return null;
    }

    public BitSet getBitSet() {
        return bitset;
    }
}

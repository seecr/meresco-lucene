package org.meresco.lucene.search.join.relational;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.FixedBitSet;
import org.meresco.lucene.Lucene;
import org.meresco.lucene.queries.KeyFilter;


public class KeyBits {
    FixedBitSet bitset;
    boolean inverted;

    public KeyBits(FixedBitSet bitSet) {
        this.bitset = bitSet;
    }

    public KeyBits(FixedBitSet bitSet, boolean inverted) {
        this.bitset = bitSet;
        this.inverted = inverted;
    }

    public KeyFilter keyFilterFor(String keyName) {
        return new KeyFilter(this.bitset, keyName, this.inverted);
    }

    public FixedBitSet getBitSet(Lucene lucene, String keyName) throws Throwable {
        FixedBitSet result = this.bitset;
        if (this.inverted) {
            result = lucene.collectKeys(this.keyFilterFor(keyName), keyName, null);
        }
        return result;
    }

    /**
     * mutates in place
     */
    public void intersect(KeyBits other) {
        FixedBitSet otherBitSet = other.bitset;
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
    public void union(KeyBits other) {
        other.normalizeBitSet(this.bitset.length());
        FixedBitSet otherBitSet = other.bitset;
        this.normalizeBitSet(otherBitSet.length());
        this.bitset.or(otherBitSet);
    }

    /**
     * mutates in place
     */
    KeyBits normalizeBitSet(int size) {
        this.bitset = FixedBitSet.ensureCapacity(this.bitset, size);
        if (this.inverted) {
            this.bitset.flip(0, this.bitset.length());  // seems expensive
            this.inverted = false;
        }
        return this;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(getClass().getSimpleName() + "(bitset=[");
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
}

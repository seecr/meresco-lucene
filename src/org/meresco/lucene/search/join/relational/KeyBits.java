/* begin license *
 *
 * "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
 *
 * Copyright (C) 2016 Seecr (Seek You Too B.V.) https://seecr.nl
 *
 * This file is part of "Meresco Lucene"
 *
 * "Meresco Lucene" is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * "Meresco Lucene" is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with "Meresco Lucene"; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
 *
 * end license */

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

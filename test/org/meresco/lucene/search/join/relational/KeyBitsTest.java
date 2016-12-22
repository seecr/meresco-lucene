package org.meresco.lucene.search.join.relational;

import static org.junit.Assert.assertEquals;

import org.apache.lucene.util.FixedBitSet;
import org.junit.Test;
import org.meresco.lucene.SeecrTestCase;


public class KeyBitsTest extends SeecrTestCase {
    @Test
    public void testIntersectSmallerIntoLarger() {
        FixedBitSet b1 = new FixedBitSet(4);
        b1.set(0);
        b1.set(1);
        KeyBits r1 = new KeyBits(b1);

        FixedBitSet b2 = new FixedBitSet(5);
        b2.set(1);
        b2.set(4);
        KeyBits r2 = new KeyBits(b2);

        r2.intersect(r1);

        b2 = r2.bitset;
        assertEquals(true, b2.get(1));
        assertEquals(false, b2.get(0));
        assertEquals(false, b2.get(4));
    }

    @Test
    public void testIntersectLargerIntoSmaller() {
        FixedBitSet b1 = new FixedBitSet(4);
        b1.set(0);
        b1.set(1);
        KeyBits r1 = new KeyBits(b1);

        FixedBitSet b2 = new FixedBitSet(5);
        b2.set(1);
        b2.set(4);
        KeyBits r2 = new KeyBits(b2);

        r1.intersect(r2);

        b1 = r1.bitset;
        assertEquals(true, b1.get(1));
        assertEquals(false, b1.get(0));
        assertEquals(false, b1.get(4));
    }

    @Test
    public void testIntersectInvertedInto() {
        FixedBitSet b1 = new FixedBitSet(4);
        b1.set(0);
        b1.set(1);
        KeyBits r1 = new KeyBits(b1);
        r1.inverted = true;

        FixedBitSet b2 = new FixedBitSet(5);
        b2.set(1);
        b2.set(4);
        KeyBits r2 = new KeyBits(b2);

        r2.intersect(r1);

        b2 = r2.bitset;
        assertEquals(false, b2.get(1));
        assertEquals(false, b2.get(0));
        assertEquals(true, b2.get(4));
    }

    @Test
    public void testIntersectIntoInverted() {
        FixedBitSet b1 = new FixedBitSet(4);
        b1.set(0);
        b1.set(1);
        KeyBits r1 = new KeyBits(b1);
        r1.inverted = true;

        FixedBitSet b2 = new FixedBitSet(5);
        b2.set(1);
        b2.set(4);
        KeyBits r2 = new KeyBits(b2);

        r1.intersect(r2);

        b1 = r1.bitset;
        assertEquals(false, b1.get(1));
        assertEquals(false, b1.get(0));
        assertEquals(true, b1.get(4));
    }

    @Test
    public void testIntersectInvertedIntoInverted() {
        FixedBitSet b1 = new FixedBitSet(4);
        b1.set(0);
        b1.set(1);
        KeyBits r1 = new KeyBits(b1);
        r1.inverted = true;

        FixedBitSet b2 = new FixedBitSet(5);
        b2.set(1);
        b2.set(4);
        KeyBits r2 = new KeyBits(b2);
        r2.inverted = true;

        r1.intersect(r2);

        b1 = r1.bitset;

        assertEquals(false, b1.get(0));
        assertEquals(false, b1.get(1));
        assertEquals(true, b1.get(2));
        assertEquals(true, b1.get(3));
        assertEquals(false, b1.get(4));
    }
}

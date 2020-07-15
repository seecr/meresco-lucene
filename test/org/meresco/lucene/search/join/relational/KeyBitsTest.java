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

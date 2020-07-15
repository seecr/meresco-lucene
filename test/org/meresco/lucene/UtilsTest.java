/* begin license *
 *
 * "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
 *
 * Copyright (C) 2016 Koninklijke Bibliotheek (KB) http://www.kb.nl
 * Copyright (C) 2016, 2019 Seecr (Seek You Too B.V.) https://seecr.nl
 * Copyright (C) 2016 Stichting Kennisnet http://www.kennisnet.nl
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

package org.meresco.lucene;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.lucene.util.FixedBitSet;
import org.junit.Before;
import org.junit.Test;

import org.meresco.lucene.Utils;
import org.meresco.lucene.Utils;

public class UtilsTest {

    @Before
    public void setUp() throws Exception {
    }

    @Test
    public void test() throws IOException {
        FixedBitSet bitSet = new FixedBitSet(501);
        bitSet.set(1);
        bitSet.set(10);
        bitSet.set(50);
        bitSet.set(500);
        
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        Utils.writeFixedBitSet(bitSet, bos);
        FixedBitSet newBitSet = Utils.readFixedBitSet(new ByteArrayInputStream(bos.toByteArray()));
        
        assertEquals(bitSet, newBitSet);
    }
    
    @Test
    public void test1() {
        FixedBitSet bitSet = new FixedBitSet(8);
        FixedBitSet bitSet1 = new FixedBitSet(9);
        bitSet1.set(8);
        assertEquals(9, bitSet1.length());
        bitSet = FixedBitSet.ensureCapacity(bitSet, bitSet1.length());
        bitSet.or(bitSet1);
        assertTrue(bitSet.get(8));
    }

    @Test
    public void testInt1120() {
        assertEquals(0, Utils.int1120ToFloat(Utils.floatToInt1120(0)), 0);
        assertEquals(0, Utils.int1120ToFloat(Utils.floatToInt1120(-1.f)), 0);

        float smallestFloat = Float.intBitsToFloat((1<<(23-11)) + ((127-20) << 23)); // 9.5414E-7
        assertEquals(smallestFloat, toInt1120AndBack(smallestFloat), 0);

        float smallerThanSmallestFloat = smallestFloat * 0.5f;
        assertEquals(smallestFloat, toInt1120AndBack(smallerThanSmallestFloat), 0);

        float biggestFloat = Float.intBitsToFloat((0xffff<<(23-11)) + ((127-20) << 23)); // 4095.0
        assertEquals(biggestFloat, toInt1120AndBack(biggestFloat), 0);

        float biggerThanBiggestFloat = biggestFloat * 2.0f;
        assertEquals(biggestFloat, toInt1120AndBack(biggerThanBiggestFloat), 0);

        assertEquals(3.140625f, toInt1120AndBack((float)Math.PI), 0);
    }

    float toInt1120AndBack(float f) {
        int i = Utils.floatToInt1120(f);
        i &= 0xffff;
        return Utils.int1120ToFloat(i);
    }
}

/* begin license *
 *
 * "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
 *
 * Copyright (C) 2016 Koninklijke Bibliotheek (KB) http://www.kb.nl
 * Copyright (C) 2016 Seecr (Seek You Too B.V.) http://seecr.nl
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

}

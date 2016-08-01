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

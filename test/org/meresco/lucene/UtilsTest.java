package org.meresco.lucene;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.lucene.util.OpenBitSet;
import org.junit.Before;
import org.junit.Test;

public class UtilsTest {

    @Before
    public void setUp() throws Exception {
    }

    @Test
    public void test() throws IOException {
        OpenBitSet bitSet = new OpenBitSet();
        bitSet.set(1L);
        bitSet.set(10L);
        bitSet.set(50L);
        bitSet.set(500L);
        
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        Utils.writeOpenBitSet(bitSet, bos);
        OpenBitSet newBitSet = Utils.readOpenBitSet(new ByteArrayInputStream(bos.toByteArray()));
        
        assertEquals(bitSet, newBitSet);
    }

}

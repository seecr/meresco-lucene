package test.org.meresco.lucene;

import static org.junit.Assert.*;

import java.util.Random;

import org.junit.Before;
import org.junit.Test;
import org.meresco.lucene.HashSetLinear;

public class HashSetLinearTest {

	private HashSetLinear hashSet;
	
	@Before
	public void setUp() {
		hashSet = new HashSetLinear(20);
	}
	
	@Test
	public void testAddAndContains() {
		for (long i=1; i<=5; i++) {
			hashSet.add(i);
		}
		for (long i=1; i<=5; i++) {
			assertTrue(hashSet.contains(i));
		}
	}

	@Test
	public void testNoContains() {
		assertFalse(hashSet.contains(1));
	}
	
	@Test
	public void testHashZeroNotAllowed() {
		try {
			hashSet.add(0);
			fail("Should raise RuntimeException");
		} catch (RuntimeException e) {
			
		}
	}
	
	@Test
	public void testAddWithDuplicateIndexes() {
		hashSet.add(1); //index 0
		hashSet.add(3); //index 1
		hashSet.add(2); //index 1; 5 > 2
		
		assertTrue(hashSet.contains(1));
		assertTrue(hashSet.contains(2));
		assertTrue(hashSet.contains(3));
	}
	
	@Test
	public void testAddWithDuplicateIndexes2() {
		hashSet.add(1); //index 5
		hashSet.add(3); //index 6
		hashSet.add(5); //index 7
		hashSet.add(7); //index 8
		hashSet.add(4); //index 6; 5 > 8; 7 > 9
		hashSet.add(2); //index 1; 3 > 2; 5 > 4; 7 > 5
		hashSet.add(8); //index 6
		hashSet.add(6); //index 5; 7 > 6; 8 > 7
		
		for (long i=1; i<=8; i++) {
			assertTrue(hashSet.contains(i));
		}
	}
	
	@Test
	public void testFullSet() {
		for (long i=1; i<=10; i++) {
			hashSet.add(i);
		}
		try {
			hashSet.add(11);
			fail("Should raise RuntimeException");
		} catch (ArrayIndexOutOfBoundsException e) {
			assertEquals("java.lang.ArrayIndexOutOfBoundsException: 20", e.toString());
		}
	}
	
	@Test
	public void testAddSameHash() {
		hashSet.add(1);
		hashSet.add(2);
		assertTrue(hashSet.contains(1));
		assertTrue(hashSet.contains(2));
		hashSet.add(1);
		
		for (long i=3; i<=10; i++) {
			hashSet.add(i);
		}
	}
	
	@Test
	public void testAddNegativeHash() {
		long hash = -(long) Math.pow(2, 63);
		hashSet.add(hash);
		assertTrue(hashSet.contains(hash));
	}
	
	@Test
	public void testRandomSet() {
		Random r = new Random();
		HashSetLinear hashSet = new HashSetLinear((int) Math.pow(2, 20));
		for (int i=0; i<10000; i++) {
			long hash = (long) (r.nextDouble()*Math.pow(2, 63));
			hashSet.add(hash);
		}
	}
	
	@Test
	public void testBigOne() {
		HashSetLinear h = new HashSetLinear(10000000);
	}
}

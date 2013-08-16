package test.org.meresco.lucene;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;
import org.meresco.lucene.HashSetLinear;

public class HashSetLinearTest {

	private HashSetLinear hashSet;
	
	@Before
	public void setUp() {
		hashSet = new HashSetLinear(10);
	}
	
	@Test
	public void testAddAndContains() {
		for (long i=1; i<=10; i++) {
			hashSet.add(i);
		}
		for (long i=1; i<=10; i++) {
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
		hashSet.add(1); //index 0
		hashSet.add(3); //index 1
		hashSet.add(5); //index 2
		hashSet.add(7); //index 3
		hashSet.add(4); //index 2; 5 > 3; 7 > 4
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
			assertEquals("java.lang.ArrayIndexOutOfBoundsException: 10", e.toString());
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
}

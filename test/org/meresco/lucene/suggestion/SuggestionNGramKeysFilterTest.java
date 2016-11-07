package org.meresco.lucene.suggestion;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.IOException;

import org.apache.lucene.util.FixedBitSet;
import org.junit.Test;


public class SuggestionNGramKeysFilterTest {
	@Test
	public void testEquals() throws IOException {
		FixedBitSet keys = new FixedBitSet(5);
		keys.set(2);
		FixedBitSet otherKeys = new FixedBitSet(4);
		SuggestionNGramKeysFilter keySetFilter = new SuggestionNGramKeysFilter(keys, "__key__");
		assertFalse(new SuggestionNGramKeysFilter(keys, "__other_key__").equals(keySetFilter));
		assertFalse(new SuggestionNGramKeysFilter(otherKeys, "__key__").equals(keySetFilter));
		assertEquals(new SuggestionNGramKeysFilter(keys, "__key__"), keySetFilter);
	}
}

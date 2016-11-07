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

package org.meresco.lucene.suggestion;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.lucene.util.FixedBitSet;
import org.junit.Before;
import org.junit.Test;
import org.meresco.lucene.SeecrTestCase;
import org.meresco.lucene.suggestion.SuggestionIndex.IndexingState;
import org.meresco.lucene.suggestion.SuggestionNGramIndex.Reader;
import org.meresco.lucene.suggestion.SuggestionNGramIndex.Suggestion;


public class SuggestionIndexTest extends SeecrTestCase {
    private SuggestionIndex index;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        String suggestionIndexDir = this.tmpDir + "/suggestions";
        String suggestionNGramIndexDir = this.tmpDir + "/ngram";
        index = new SuggestionIndex(suggestionIndexDir, suggestionNGramIndexDir, 2, 4);
    }

    @Test
    public void testSuggestionsAreEmptyIfNotCreated() throws Exception {
        addSuggestions("id:1", 1);
        assertEquals(0, index.getSuggestionsReader().suggest("ha", false, null).length);
    }

    @Test
    public void testSuggest() throws Exception {
        addSuggestions("id:1", 1);
        index.createSuggestionNGramIndex(true, false);

        Reader reader = index.getSuggestionsReader();
        Suggestion[] suggestions = reader.suggest("ha", false, null);
        assertEquals(2, suggestions.length);
        assertEquals("hallo", suggestions[0].suggestion);
        assertEquals("harry", suggestions[1].suggestion);

        suggestions = reader.suggest("fiet", false, null);
        assertEquals(2, suggestions.length);
        assertEquals("fiets", suggestions[0].suggestion);
        assertEquals("fiets mobiel", suggestions[1].suggestion);

        assertEquals(5, reader.numDocs());
    }

    @Test
    public void testSuggestWithTypesAndCreators() throws Exception {
        addSuggestions("id:1", 1);
        index.createSuggestionNGramIndex(true, false);

        Reader reader = index.getSuggestionsReader();
        Suggestion[] suggestions = reader.suggest("ha", false, null);

        assertEquals("hallo", suggestions[0].suggestion);
        assertEquals("uri:book", suggestions[0].type);
        assertEquals("by:me", suggestions[0].creator);

        assertEquals("harry", suggestions[1].suggestion);
        assertEquals("uri:book", suggestions[1].type);
        assertEquals("rowling", suggestions[1].creator);
    }

    @Test
    public void testPersistentShingles() throws Exception {
        addSuggestions("id:1", 1);
        index.createSuggestionNGramIndex(true, false);
        index.close();

        String suggestionIndexDir = this.tmpDir + "/suggestions";
        String suggestionNGramIndexDir = this.tmpDir + "/ngram";
        index = new SuggestionIndex(suggestionIndexDir, suggestionNGramIndexDir, 2, 4);
        Suggestion[] suggestions = index.getSuggestionsReader().suggest("ha", false, null, null, 4);
        assertEquals("hallo", suggestions[0].suggestion);
        assertEquals("harry", suggestions[1].suggestion);
    }

    public void testAddDelete() throws IOException {
        addSuggestions("id:1", 1);
        addSuggestions("id:2", 1);
        assertEquals(2, index.numDocs());
        index.delete("id:2");
        assertEquals(1, index.numDocs());
    }

    public void testFilterByKeySet() throws Exception {
        addSuggestions("id:1", 1);
        index.add("id:2", 2, new String[] {"fietsbel"}, new String[]{"uri:book"}, new String[] {"Anonymous"});
        index.createSuggestionNGramIndex(true, false);

        FixedBitSet bits = new FixedBitSet(3);
        bits.set(2);
        index.registerFilterKeySet("apikey-abc", bits);

        Suggestion[] suggestions = index.getSuggestionsReader().suggest("fi", false, null, "apikey-abc", 5);
        assertEquals(1, suggestions.length);
        assertEquals("fietsbel", suggestions[0].suggestion);

        suggestions = index.getSuggestionsReader().suggest("fi", false, null, "apikey-never-registered", 5);
        assertEquals(3, suggestions.length);
        assertEquals("fiets", suggestions[0].suggestion);
        assertEquals("fietsbel", suggestions[1].suggestion);
        assertEquals("fiets mobiel", suggestions[2].suggestion);

        suggestions = index.getSuggestionsReader().suggest("fi", false, null, "apikey-never-registered", 2);
        assertEquals(2, suggestions.length);
    }

    public void assertSuggestion(String input, String[] expected, boolean trigram) throws Exception {
        Reader reader = index.getSuggestionsReader();
        Suggestion[] suggestions = reader.suggest(input, trigram, null);
        String[] result = new String[suggestions.length];
        for (int i=0; i<suggestions.length; i++) {
            result[i] = suggestions[i].suggestion;
        }
        assertArrayEquals(expected, result);
    }

    @Test
    public void testFindShingles() throws IOException {
        List<String> shingles = index.shingles("Lord of the rings");
        assertEquals(Arrays.asList("lord", "lord of", "lord of the", "lord of the rings", "of", "of the", "of the rings", "the", "the rings", "rings" ), shingles);
    }

    @Test
    public void testFindNgramsForShingle() throws IOException {
        List<String> ngrams = SuggestionNGramIndex.ngrams("lord", false);
        assertEquals(Arrays.asList("$l", "lo", "or", "rd", "d$"), ngrams);
        ngrams = SuggestionNGramIndex.ngrams("lord", true);
        assertEquals(Arrays.asList("$lo", "lor", "ord", "rd$"), ngrams);
        ngrams = SuggestionNGramIndex.ngrams("lord of", false);
        assertEquals(Arrays.asList("$l", "lo", "or", "rd", "d$", "$o", "of", "f$"), ngrams);
        ngrams = SuggestionNGramIndex.ngrams("lord of", true);
        assertEquals(Arrays.asList("$lo", "lor", "ord", "rd$", "$of", "of$"), ngrams);
    }

    @Test
    public void testSuggestionIndex() throws Exception {
        index.add("identifier", 1, new String[] {"Lord of the rings", "Fellowship of the ring"}, new String[] {null, null}, new String[] {null, null});
        index.createSuggestionNGramIndex(true, false);

        assertSuggestion("l", new String[] {"Lord of the rings"}, false);
        assertSuggestion("l", new String[] {}, true);
        assertSuggestion("lord", new String[] {"Lord of the rings"}, false);
        assertSuggestion("lord of", new String[] {"Lord of the rings"}, false);
        assertSuggestion("of the", new String[] {"Lord of the rings", "Fellowship of the ring"}, false);
        assertSuggestion("fel", new String[] {"Fellowship of the ring"}, false);
    }

    @Test
    public void testRanking() throws Exception {
        index.add("identifier", 1, new String[] {"Lord of the rings", "Lord magic"}, new String[] {null, null}, new String[] {null, null});
        index.add("identifier2", 2, new String[] {"Lord of the rings"}, new String[] {null}, new String[] {null});
        index.add("identifier3", 3, new String[] {"Lord magic"}, new String[] {null}, new String[] {null});
        index.add("identifier4", 4, new String[] {"Lord magic"}, new String[] {null}, new String[] {null});
        index.createSuggestionNGramIndex(true, false);

        Reader reader = index.getSuggestionsReader();
        Suggestion[] suggestions = reader.suggest("lo", false, null);
        assertEquals(2, suggestions.length);
        assertEquals("Lord magic", suggestions[0].suggestion);
        assertEquals("Lord of the rings", suggestions[1].suggestion);
        assertEquals(0.3498380780220032, suggestions[0].score, 0);
        assertEquals( 0.30888697504997253, suggestions[1].score, 0);
    }

    @Test
    public void testCreatingIndexState() throws Exception {
        assertEquals(null, index.indexingState());
        for (int i=0; i<100; i++)
            index.add("identifier" + i, 1,new String[] {"Lord rings", "Lord magic"}, new String[] {null, null}, new String[] {null, null});
        try {
            index.createSuggestionNGramIndex(false, false);
            Thread.sleep(5); // Wait for thread
            IndexingState state = index.indexingState();
            assertNotEquals(null, state);
            assertTrue(0 <= state.count);
            assertTrue(100 > state.count);
        } finally {
            Thread.sleep(100); // Wait for thread
        }
    }

    @Test
    public void testCreateSuggestionsForEmptyIndex() throws IOException {
        index.createSuggestionNGramIndex(true, false);
    }

    @Test
    public void testSuggestionWithConceptTypesAndCreators() throws Exception {
        index.add("identifier", 1, new String[] {"Lord of the rings", "Lord magic"}, new String[] {"uri:book", null}, new String[] {"uri:author", null});
        index.createSuggestionNGramIndex(true, false);

        Reader reader = index.getSuggestionsReader();
        Suggestion[] suggestions = reader.suggest("lo", false, null);
        assertEquals(2, suggestions.length);
        assertEquals("Lord magic", suggestions[0].suggestion);
        assertEquals("Lord of the rings", suggestions[1].suggestion);
        assertEquals(null, suggestions[0].type);
        assertEquals("uri:book", suggestions[1].type);
        assertEquals(null, suggestions[0].creator);
        assertEquals("uri:author", suggestions[1].creator);
    }

    @Test
    public void testSuggestWithFilter() throws Exception {
        index.add("identifier", 1, new String[] {"Lord of the rings", "Lord magic"}, new String[] {"uri:book", null}, new String[] {null, null});
        index.createSuggestionNGramIndex(true, false);;

        Reader reader = index.getSuggestionsReader();
        Suggestion[] suggestions = reader.suggest("lo", false, new String[] {"type=uri:book"});
        assertEquals(1, suggestions.length);
        assertEquals("Lord of the rings", suggestions[0].suggestion);
    }

    @Test
    public void testCommit() throws Exception {
        index.add("identifier", 1, new String[] {"Lord of the rings", "Fellowship of the ring"}, new String[] {null, null}, new String[] {null, null});
        index.createSuggestionNGramIndex(true, false);;
        index.commit();
        assertSuggestion("l", new String[] {"Lord of the rings"}, false);
    }

    public void addSuggestions(String identifier, int key) throws IOException {
        String[] values = { "harry", "potter", "hallo", "fiets", "fiets mobiel" };
        String[] types = { "uri:book", "uri:book", "uri:book", "uri:book", "uri:book" };
        String[] creators = { "rowling", "rowling", "by:me", "by:me", null };
        index.add(identifier, key, values, types, creators);
    }
}

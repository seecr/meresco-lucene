## begin license ##
#
# "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
#
# Copyright (C) 2015 Koninklijke Bibliotheek (KB) http://www.kb.nl
# Copyright (C) 2015 Seecr (Seek You Too B.V.) http://seecr.nl
#
# This file is part of "Meresco Lucene"
#
# "Meresco Lucene" is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# "Meresco Lucene" is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with "Meresco Lucene"; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
#
## end license ##

from os import remove
from os.path import join, isfile
from time import sleep

from seecr.test import SeecrTestCase

from org.apache.lucene.index import Term
from org.apache.lucene.search import QueryWrapperFilter, TermQuery
from org.meresco.lucene.suggestion import SuggestionIndex, SuggestionNGramIndex


class SuggestionIndexTest(SeecrTestCase):
    def setUp(self):
        super(SuggestionIndexTest, self).setUp()
        suggestionIndexDir = join(self.tempdir, "shingles")
        ngramIndexDir = join(self.tempdir, "suggestions")
        self._suggestionIndex = SuggestionIndex(suggestionIndexDir, ngramIndexDir, 2, 4)

    def assertSuggestion(self, input, expected, trigram=False):
        reader = self._suggestionIndex.getSuggestionsReader()
        suggestions = [s.suggestion for s in reader.suggest(input, trigram, None)]
        self.assertEquals(set(expected), set(suggestions))

    def testFindShingles(self):
        shingles = self._suggestionIndex.shingles("Lord of the rings")
        self.assertEquals(["lord", "lord of", "lord of the", "lord of the rings", "of", "of the", "of the rings", "the", "the rings", "rings"], list(shingles))

    def testFindNgramsForShingle(self):
        s = SuggestionNGramIndex(self.tempdir)
        ngrams = s.ngrams("lord", False)
        self.assertEquals(["$l", "lo", "or", "rd", "d$"], list(ngrams))
        ngrams = s.ngrams("lord", True)
        self.assertEquals(["$lo", "lor", "ord", "rd$"], list(ngrams))
        ngrams = s.ngrams("lord of", False)
        self.assertEquals(["$l", "lo", "or", "rd", "d$", "$o", "of", "f$"], list(ngrams))
        ngrams = s.ngrams("lord of", True)
        self.assertEquals(["$lo", "lor", "ord", "rd$", "$of", "of$"], list(ngrams))

    def testSuggestionIndex(self):
        self._suggestionIndex.add("identifier", ["Lord of the rings", "Fellowship of the ring"], [None, None], [None, None])
        self._suggestionIndex.createSuggestionNGramIndex(True, False)

        self.assertSuggestion("l", ["Lord of the rings"])
        self.assertSuggestion("l", [], trigram=True)
        self.assertSuggestion("lord", ["Lord of the rings"])
        self.assertSuggestion("lord of", ["Lord of the rings"])
        self.assertSuggestion("of the", ["Lord of the rings", "Fellowship of the ring"])
        self.assertSuggestion("fel", ['Fellowship of the ring'])

    def testRanking(self):
        self._suggestionIndex.add("identifier", ["Lord of the rings", "Lord magic"], [None]*2, [None]*2)
        self._suggestionIndex.add("identifier2", ["Lord of the rings"], [None], [None])
        self._suggestionIndex.add("identifier3", ["Lord magic"], [None], [None])
        self._suggestionIndex.add("identifier4", ["Lord magic"], [None], [None])
        self._suggestionIndex.createSuggestionNGramIndex(True, False)

        reader = self._suggestionIndex.getSuggestionsReader()
        suggestions = list(reader.suggest("lo", False, None))
        self.assertEquals(2, len(suggestions))
        self.assertEquals([u'Lord magic', u'Lord of the rings'], [s.suggestion for s in suggestions])
        self.assertEquals([0.21019981801509857, 0.1839248389005661], [s.score for s in suggestions])

    def testCreatingIndexState(self):
        self.assertEquals(None, self._suggestionIndex.indexingState())
        for i in range(100):
            self._suggestionIndex.add("identifier%s", ["Lord rings", "Lord magic"], [None]*2, [None]*2)
        try:
            self._suggestionIndex.createSuggestionNGramIndex(False, False)
            sleep(0.005) # Wait for thread
            state = self._suggestionIndex.indexingState()
            self.assertNotEquals(None, state)
            self.assertTrue(0 <= int(state.count) < 100, state)
        finally:
            sleep(0.1) # Wait for thread

    def testCreateSuggestionsForEmptyIndex(self):
        self._suggestionIndex.createSuggestionNGramIndex(True, False)
        self.assertTrue("Nothing bad happened, no NullPointerException")

    def testSuggestionWithConceptTypesAndCreators(self):
        self._suggestionIndex.add("identifier", ["Lord of the rings", "Lord magic"], ["uri:book", None], ["uri:author", None])
        self._suggestionIndex.createSuggestionNGramIndex(True, False)

        reader = self._suggestionIndex.getSuggestionsReader()
        suggestions = list(reader.suggest("lo", False, None))
        self.assertEquals(2, len(suggestions))
        self.assertEquals([u'Lord magic', u'Lord of the rings'], [s.suggestion for s in suggestions])
        self.assertEquals([None, u'uri:book'], [s.type for s in suggestions])
        self.assertEquals([None, u'uri:author'], [s.creator for s in suggestions])

    def testSuggestWithFilter(self):
        self._suggestionIndex.add("identifier", ["Lord of the rings", "Lord magic"], ["uri:book", None], [None]*2)
        self._suggestionIndex.createSuggestionNGramIndex(True, False)

        reader = self._suggestionIndex.getSuggestionsReader()
        suggestions = list(reader.suggest("lo", False, QueryWrapperFilter(TermQuery(Term("type", "uri:book")))))
        self.assertEquals(1, len(suggestions))
        self.assertEquals([u'Lord of the rings'], [s.suggestion for s in suggestions])

    def testCommit(self):
        self._suggestionIndex.add("identifier", ["Lord of the rings", "Fellowship of the ring"], [None, None], [None, None])
        self._suggestionIndex.createSuggestionNGramIndex(True, False)
        self._suggestionIndex.commit()
        self.assertSuggestion("l", ["Lord of the rings"])

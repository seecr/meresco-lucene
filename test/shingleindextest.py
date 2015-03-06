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

from seecr.test import SeecrTestCase

from org.meresco.lucene.suggestion import ShingleIndex, SuggestionIndex
from os.path import join
from time import sleep

class ShingleIndexTest(SeecrTestCase):

    def setUp(self):
        super(ShingleIndexTest, self).setUp()
        shingleIndexDir = join(self.tempdir, "shingles")
        suggestionIndexDir = join(self.tempdir, "suggestions")
        self._shingleIndex = ShingleIndex(shingleIndexDir, suggestionIndexDir, 2, 4)

    def assertSuggestion(self, suggest, expected, trigram=False):
        reader = self._shingleIndex.getSuggestionsReader()
        suggestions = [s.suggestion for s in reader.suggest(suggest, trigram)]
        self.assertEquals(set(expected), set(suggestions))

    def testFindShingles(self):
        shingles = self._shingleIndex.shingles("Lord of the rings")
        self.assertEquals(["lord", "lord of", "lord of the", "lord of the rings", "of", "of the", "of the rings", "the", "the rings", "rings"], list(shingles))

    def testFindNgramsForShingle(self):
        s = SuggestionIndex(self.tempdir)
        ngrams = s.ngrams("lord", False)
        self.assertEquals(["$l", "lo", "or", "rd", "d$"], list(ngrams))
        ngrams = s.ngrams("lord", True)
        self.assertEquals(["$lo", "lor", "ord", "rd$"], list(ngrams))
        ngrams = s.ngrams("lord of", False)
        self.assertEquals(["$l", "lo", "or", "rd", "d$", "$o", "of", "f$"], list(ngrams))
        ngrams = s.ngrams("lord of", True)
        self.assertEquals(["$lo", "lor", "ord", "rd$", "$of", "of$"], list(ngrams))

    def testShingleIndex(self):
        self._shingleIndex.add("identifier", ["Lord of the rings", "Fellowship of the ring"])
        self._shingleIndex.createSuggestionIndex(True)

        self.assertSuggestion("l", ["lord of the rings", "lord of the", "lord of", "lord"])
        self.assertSuggestion("l", [], trigram=True)
        self.assertSuggestion("lord", ["lord of the rings", "lord of the", "lord of", "lord"])
        self.assertSuggestion("lord of", ["lord of the rings", "lord of the", "lord of"])
        self.assertSuggestion("of the", ['fellowship of the ring', 'fellowship of the', "lord of the rings", "lord of the", "of the", "of the ring", "of the rings"])
        self.assertSuggestion("fel", ['fellowship of the ring', 'fellowship of the', 'fellowship of', 'fellowship'])

    def testShingleInMultipleDocumentsRanksHigher(self):
        self._shingleIndex.add("identifier", ["Lord rings", "Lord magic"])
        self._shingleIndex.add("identifier2", ["Lord rings"])
        self._shingleIndex.add("identifier3", ["Lord magic"])
        self._shingleIndex.add("identifier4", ["Lord magic"])
        self._shingleIndex.createSuggestionIndex(True)

        reader = self._shingleIndex.getSuggestionsReader()
        suggestions = list(reader.suggest("lo", False))
        self.assertEquals(3, len(suggestions))
        self.assertEquals(['lord', 'lord magic', 'lord rings'], [s.suggestion for s in suggestions])
        self.assertEquals([0.1420000046491623, 0.11299999803304672, 0.0729999989271164], [s.score for s in suggestions])

    def testSuggestFromLongDescription(self):
        self.maxDiff = None
        description = "Een jonge alleenstaande moeder moet kiezen tussen haar betrouwbare vriend en de botte biologische vader van haar dochtertje."
        self._shingleIndex.add("identifier", [description])
        self._shingleIndex.createSuggestionIndex(True)

        self.assertSuggestion("een jong", ['een jonge alleenstaande moeder', 'een jonge alleenstaande', 'een jonge'])
        self.assertSuggestion("botte", [
                'botte biologische vader van',
                'botte biologische vader',
                'de botte biologische',
                'botte biologische',
                'de botte',
                'botte',
                'de botte biologische vader',
                'en de botte',
                'en de botte biologische',
                'vriend en de botte'
            ])

    def testCreatingIndexState(self):
        self.assertEquals(None, self._shingleIndex.indexingState())
        for i in range(100):
            self._shingleIndex.add("identifier%s", ["Lord rings", "Lord magic"])
        try:
            self._shingleIndex.createSuggestionIndex(False)
            sleep(0.01) # Wait for thread
            state = self._shingleIndex.indexingState()
            self.assertNotEquals(None, state)
            self.assertTrue(0 <= int(state.count) < 100, state)
        finally:
            sleep(0.1) # Wait for thread

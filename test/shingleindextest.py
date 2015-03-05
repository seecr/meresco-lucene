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

class ShingleIndexTest(SeecrTestCase):

    def testFindShingles(self):
        s = ShingleIndex(self.tempdir, 2, 4)
        shingles = s.shingles("Lord of the rings")
        self.assertEquals(["lord", "lord of", "lord of the", "lord of the rings", "of", "of the", "of the rings", "the", "the rings", "rings"], list(shingles))

    def testFindNgramsForShingle(self):
        s = SuggestionIndex(self.tempdir, 2, 3)
        ngrams = s.ngrams("lord", False)
        self.assertEquals(["$l", "lo", "or", "rd", "d$"], list(ngrams))
        ngrams = s.ngrams("lord", True)
        self.assertEquals(["$lo", "lor", "ord", "rd$"], list(ngrams))
        ngrams = s.ngrams("lord of", False)
        self.assertEquals(["$l", "lo", "or", "rd", "d$", "$o", "of", "f$"], list(ngrams))
        ngrams = s.ngrams("lord of", True)
        self.assertEquals(["$lo", "lor", "ord", "rd$", "$of", "of$"], list(ngrams))

    def testShingleIndex(self):
        s = ShingleIndex(self.tempdir, 2, 4)
        s.add("identifier", ["Lord of the rings", "Fellowship of the ring"])
        s.commit()

        suggestionIndexDir = join(self.tempdir, "suggestions")
        suggestionIndex = s.createSuggestionIndex(suggestionIndexDir)

        reader = suggestionIndex.getReader()
        self.assertEquals([u"lord of the rings", u"lord of the", u"lord of", u"lord"], list(reader.suggest("l", False)))
        self.assertEquals([], list(reader.suggest("l", True)))
        self.assertEquals([u"lord of the rings", u"lord of the", u"lord of", u"lord"], list(reader.suggest("lord", False)))
        self.assertEquals([u"lord of the rings", u"lord of the", u"lord of"], list(reader.suggest("lord of", False)))
        self.assertEquals(['fellowship of the ring', 'fellowship of the', "lord of the rings", "lord of the", "of the", "of the ring", "of the rings", ], list(reader.suggest("of the", False)))
        self.assertEquals(['fellowship of the ring', 'fellowship of the', 'fellowship of', 'fellowship'], list(reader.suggest("fel", False)))

    def testShingleInMultipleDocumentsRanksHigherIndex(self):
        s = ShingleIndex(self.tempdir, 2, 4)
        s.add("identifier", ["Lord rings", "Lord magic"])
        s.add("identifier2", ["Lord rings"])
        s.add("identifier3", ["Lord magic"])
        s.add("identifier4", ["Lord magic"])
        s.commit()

        suggestionIndexDir = join(self.tempdir, "suggestions")
        suggestionIndex = s.createSuggestionIndex(suggestionIndexDir)

        reader = suggestionIndex.getReader()
        self.assertEquals(['lord', 'lord magic', 'lord rings'], list(reader.suggest("lo", False)))

    def testSuggestFromLongDescription(self):
        self.maxDiff = None
        description = "Een jonge alleenstaande moeder moet kiezen tussen haar betrouwbare vriend en de botte biologische vader van haar dochtertje."
        # self.assertEquals([], list(ShingleIndex.shingles(description)))
        s = ShingleIndex(self.tempdir, 2, 4)
        s.add("identifier", [description])
        s.commit()
        suggestionIndexDir = join(self.tempdir, "suggestions")
        suggestionIndex = s.createSuggestionIndex(suggestionIndexDir)

        reader = suggestionIndex.getReader()
        self.assertEquals(['een jonge alleenstaande moeder', 'een jonge alleenstaande', 'een jonge'], list(reader.suggest("een jonge", False)))
        self.assertEquals([
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
            ], list(reader.suggest("botte", False)))

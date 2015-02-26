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

from org.meresco.lucene.suggestion import ShingleIndex

class ShingleIndexTest(SeecrTestCase):

    def testShingleIndex(self):
        s = ShingleIndex(self.tempdir, 2, 4)
        s.add("Lord of the rings")

        self.assertEquals(["lord of", "lord of the", "lord of the rings"], list(s.suggest("lord")))
        self.assertEquals(["lord of the", "lord of the rings"], list(s.suggest("lord of")))
        self.assertEquals(["of the rings"], list(s.suggest("of the")))

    def testSuggestFromLongDescription(self):
        self.maxDiff = None
        description = "Een jonge alleenstaande moeder moet kiezen tussen haar betrouwbare vriend en de botte biologische vader van haar dochtertje."
        # self.assertEquals([], list(ShingleIndex.shingles(description)))
        s = ShingleIndex(self.tempdir, 2, 4)
        s.add(description)
        self.assertEquals(["een jonge alleenstaande", "een jonge alleenstaande moeder"], list(s.suggest("een jonge")))
        self.assertEquals(["botte biologische", "botte biologische vader", "botte biologische vader van"], list(s.suggest("botte")))

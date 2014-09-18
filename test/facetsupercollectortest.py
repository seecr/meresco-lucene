## begin license ##
#
# "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
#
# Copyright (C) 2014 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
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
from org.meresco.lucene.search import FacetSuperCollector
from org.apache.lucene.facet import FacetsConfig
from org.apache.lucene.facet.taxonomy import CachedOrdinalsReader, DocValuesOrdinalsReader

class FacetSuperCollectorTest(SeecrTestCase):

    def testEmpty(self):
        f = FacetSuperCollector(None, FacetsConfig(), CachedOrdinalsReader(DocValuesOrdinalsReader()))
        self.assertEquals(None, f.getFirstArray())

    def testOneArray(self):
        f = FacetSuperCollector(None, FacetsConfig(), CachedOrdinalsReader(DocValuesOrdinalsReader()))
        f.mergePool([0, 1, 2, 3, 4])
        self.assertEquals([0, 1, 2, 3, 4], f.getFirstArray())

    def testMergeTwoArray(self):
        f = FacetSuperCollector(None, FacetsConfig(), CachedOrdinalsReader(DocValuesOrdinalsReader()))
        f.mergePool([0, 1, 2, 3, 4])
        f.mergePool([0, 0, 1, 1, 1])
        self.assertEquals([0, 1, 3, 4, 5], f.getFirstArray())
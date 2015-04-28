## begin license ##
#
# "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
#
# Copyright (C) 2013 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2013 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
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
from org.meresco.lucene.search import GeneralizedJaccardDistance

class GeneralizedJaccardDistanceTest(SeecrTestCase):

    def testOne(self):
        J = GeneralizedJaccardDistance()
        self.assertEqual(0.00, J.compute([1.0], [1.0]))
        self.assertEqual(1.00, J.compute([], [1.0]))
        self.assertEqual(1.00, J.compute([1.0], []))
        self.assertEqual(0.00, J.compute([1.0, 2.0], [1.0, 2.0]))
        self.assertEqual(0.50, J.compute([1.0], [2.0]))  # 1.0 / 2.0
        self.assertEqual(0.75, J.compute([2.0], [8.0]))  # 2.0 / 8.0
        self.assertEqual(0.75, J.compute([8.0], [2.0]))  # 2.0 / 8.0
        self.assertEqual(0.50, J.compute([1.0, 2.0], [3.0, 3.0]))  # 1.0 + 2.0 / 3.0 + 3.0 = 0.5
        self.assertEqual(0.50, J.compute([1.0, 2.0], [3.0, 3.0]))  # 1.0 + 2.0 / 3.0 + 3.0 = 0.5

    def testNaN(self):
        # not sure if this is OK, but this is how it is right now
        J = GeneralizedJaccardDistance()
        nan = J.compute([], [])
        self.assertNotEqual(nan, nan)  # by IEEE 754

    def testNegativeNoException(self):
        # NOT OK, since this algorithm requires al elements >= 0.0 for correct results
        J = GeneralizedJaccardDistance()
        self.assertEqual(2.00, J.compute([1.0], [-1.0]))
        self.assertEqual(2.00, J.compute([-1.0], [1.0]))

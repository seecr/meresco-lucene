# -*- encoding: utf-8 -*-
# # begin license ##
#
# "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
#
# Copyright (C) 2013-2015 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2013-2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
# Copyright (C) 2015 Koninklijke Bibliotheek (KB) http://www.kb.nl
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
# # end license ##


from seecr.test import SeecrTestCase, CallTrace

from org.meresco.lucene.search import PageRank


class PageRankTest(SeecrTestCase):

    def XXXtestCreate(self):
        pr = PageRank()
        pr.add(5, [0.3, 0.0, 0.4])  # docid, termvector
        pr.add(3, [0.2, 0.4])
        nodes = pr.node_count();  # graph contains 2 docs and 3 terms
        self.assertEquals(5, nodes)
        pr.add(6, [0.3, 0.4, 0.4, 0.0, 0.1])
        nodes = pr.node_count();  # graph contains 3 docs and 4 terms
        self.assertEquals(7, nodes)
        pr.add(2, [0.0, 0.0, 0.0, 0.0, 0.0])
        nodes = pr.node_count();  # graph contains 3 docs and 4 terms
        self.assertEquals(8, nodes)

    def XXXtestUniqueDocId(self):
        pr = PageRank()
        pr.add(5, [1.0])
        try:
            pr.add(5, [1.0])
            self.fail("must not succeed")
        except Exception, e:
            self.assertEquals("java.lang.RuntimeException: duplicate docid 5", str(e.message))


    def XXXtestTermRanks(self):
        pr = PageRank()
        pr.add(5, [0.3, 0.0, 0.4])  # docid, termvector
        pr.add(3, [0.2, 0.4])
        R = 1.0 / pr.node_count()
        term_ranks = pr.getDocs()
        self.assertEquals(R, term_ranks.get(0))
        self.assertEquals(R, term_ranks.get(1))
        self.assertEquals(R, term_ranks.get(2))

    def testDocRanks(self):
        pr = PageRank()
        pr.add(5, [0.3, 0.0, 0.4])  # docid, termvector
        pr.add(3, [0.2, 0.4])
        pr.add(6, [0.3, 0.4, 0.4, 0.0, 0.1])
        pr.add(2, [0.0, 0.0, 0.0, 0.0, 0.0])
        pr.add(1, [0.2, 1.0, 2.0, 0.4, 0.0])
        pr.prepare()
        topDocs = pr.topDocs()
        self.assertEquals([0.1] * 5, [td.getPR() for td in topDocs])
        self.assertEquals([5, 3, 6, 2, 1], [td.id for td in topDocs])
        pr.iterate()
        topDocs = pr.topDocs()
        self.assertEquals([0.27325000000000005, 0.1875416666666667, 0.16770833333333335, 0.16558333333333336, 0.15000000000000002] , [td.getPR() for td in topDocs])
        self.assertEquals([1, 6, 5, 3, 2], [td.id for td in topDocs])


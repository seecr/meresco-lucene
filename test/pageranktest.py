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


from seecr.test import SeecrTestCase

from org.meresco.lucene.search import PageRank


class PageRankTest(SeecrTestCase):

    def testCreate(self):
        pr = PageRank(5)
        pr.add(5, [0.3, 0.0, 0.4])  # docid, termvector
        pr.add(3, [0.2, 0.4])
        nodes = pr.node_count;  # graph contains 2 docs and 3 terms
        self.assertEquals(5, nodes)
        pr.add(6, [0.3, 0.4, 0.4, 0.0, 0.1])
        nodes = pr.node_count;  # graph contains 3 docs and 4 terms
        self.assertEquals(7, nodes)
        pr.add(2, [0.0, 0.0, 0.0, 0.0, 0.0])
        nodes = pr.node_count;  # graph contains 3 docs and 4 terms
        self.assertEquals(8, nodes)
        try:
            pr.add(9, [1.0, 2.0, 3.0, 4.0, 5.0])
        except Exception, e:
            self.assertTrue("ArrayIndexOutOfBoundsException: 4" in e.getMessage(), str(e))

    def XXXtestUniqueDocId(self):
        '''Requires a reparate datastructure; forget it'''
        pr = PageRank(2)
        pr.add(5, [1.0])
        try:
            pr.add(5, [1.0])
            self.fail("must not succeed")
        except Exception, e:
            self.assertEquals("java.lang.RuntimeException: duplicate docid 5", str(e.message))

    def testDocRanks(self):
        pr = PageRank(5)
        pr.add(50, [0.3, 0.0, 0.4])  # docid, termvector
        pr.add(30, [0.2, 0.4])
        pr.add(60, [0.3, 0.4, 0.4, 0.0, 0.1])
        pr.add(20, [0.0, 0.0, 0.0, 0.0, 0.0])
        pr.add(10, [0.2, 1.0, 2.0, 0.4, 0.0])
        pr.prepare()
        self.assertEquals(10, pr.node_count)
        topDocs = pr.topDocs()
        topTerms = pr.topTerms()
        P = 1.0 / 10
        self.assertEquals([P, P, P, P, P], [node.getPR() for node in topDocs])
        self.assertEqual([0, 1, 2, 3, 4], [node.id for node in topTerms])
        self.assertEquals([50, 30, 60, 20, 10], [node.id for node in topDocs])
        self.assertEquals([2, 2, 4, 0, 4], [node.edges for node in topDocs])
        self.assertEquals([4, 3, 3, 1, 1], [node.edges for node in topTerms])
        pr.iterate()
        topDocs = pr.topDocs()
        topTerms = pr.topTerms()
        self.assertEquals([10, 60, 50, 30, 20], [node.id for node in topDocs])
        self.assertEquals([0.27325000000000005, 0.1875416666666667, 0.16770833333333335, 0.16558333333333336, 0.15000000000000002],
                          [node.getPR() for node in topDocs])
        self.assertEqual([2, 1, 0, 3, 4], [node.id for node in topTerms])
        self.assertEquals([0.21800000000000003, 0.19675000000000004, 0.181875, 0.15850000000000003, 0.152125],
                            [node.getPR() for node in topTerms])
        self.assertEquals([4, 4, 2, 2, 0], [node.edges for node in topDocs])
        self.assertEquals([3, 3, 4, 1, 1], [node.edges for node in topTerms])
        pr.iterate()
        topDocs = pr.topDocs()
        topTerms = pr.topTerms()
        self.assertEquals([10, 60, 50, 30, 20], [node.id for node in topDocs])
        self.assertEquals([0.3908988541666667, 0.22153015625000003, 0.1863011979166667, 0.18002802083333336, 0.15000000000000002],
                           [node.getPR() for node in topDocs])
        self.assertEqual([2, 1, 0, 3, 4], [node.id for node in topTerms])
        self.assertEquals([0.31058270833333335, 0.25215583333333336, 0.20902630208333337, 0.17322625000000003, 0.15398526041666669],
                            [node.getPR() for node in topTerms])
        self.assertEquals([4, 4, 2, 2, 0], [node.edges for node in topDocs])
        self.assertEquals([3, 3, 4, 1, 1], [node.edges for node in topTerms])


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

from java.util.concurrent import Executors
from lucenetest import document, createDocument
from meresco.lucene.index import Index
from org.apache.lucene.search import MatchAllDocsQuery
from org.apache.lucene.facet import FacetsConfig
from org.meresco.lucene.search import SuperCollector, SubCollector, \
    CountingSuperCollector, TopDocsSuperCollector, FacetSuperCollector


class SuperCollectorTest(SeecrTestCase):

    def setUp(self):
        super(SuperCollectorTest, self).setUp()
        self.E = Executors.newFixedThreadPool(2)

    def tearDown(self):
        self.E.shutdown()
        super(SuperCollectorTest, self).tearDown()

    def testCreate(self):
        C = CountingSuperCollector()
        self.assertTrue(isinstance(C, SuperCollector))
        S = C.subCollector(None)
        self.assertTrue(isinstance(S, SubCollector))

    def testSearch(self):
        C = CountingSuperCollector()
        I = Index(path=self.tempdir, reactor=None, executor=self.E)
        Q = MatchAllDocsQuery()
        I.search(Q, None, C)
        self.assertEquals(0, C.count())
        I._indexWriter.addDocument(document(name="one", price="2"))
        I.close()
        I = Index(path=self.tempdir, reactor=None, executor=self.E)
        I.search(Q, None, C)
        self.assertEquals(1, C.count())

    def testTopDocsSuperCollector(self):
        C = TopDocsSuperCollector(10, True)
        self.assertTrue(isinstance(C, SuperCollector))
        S = C.subCollector(None)
        self.assertTrue(isinstance(S, SubCollector))
        self.assertFalse(S.acceptsDocsOutOfOrder())
        C = TopDocsSuperCollector(10, False)
        S = C.subCollector(None)
        self.assertTrue(S.acceptsDocsOutOfOrder())

    def testSearchTopDocs(self):
        I = Index(path=self.tempdir, reactor=None, executor=self.E)
        I._indexWriter.addDocument(document(name="one", price="aap noot mies"))
        I._indexWriter.addDocument(document(name="two", price="aap vuur boom"))
        I._indexWriter.addDocument(document(name="three", price="noot boom mies"))
        I.close()
        I = Index(path=self.tempdir, reactor=None, executor=self.E)
        C = TopDocsSuperCollector(1, True)
        Q = MatchAllDocsQuery()
        I.search(Q, None, C)
        td = C.topDocs()
        self.assertEquals(3, td.totalHits)
        self.assertEquals(1, len(td.scoreDocs))

    def testFacetSuperCollector(self):
        I = Index(path=self.tempdir, reactor=None, executor=self.E)
        for i in xrange(10000):
            document1 = createDocument(fields=[("field1", str(i)), ("field2", str(i)*1000)], facets=[("facet1", "value%s" % (i % 100))])
            document1 = I._facetsConfig.build(I._taxoWriter, document1)
            I._indexWriter.addDocument(document1)
        I.close()
        I = Index(path=self.tempdir, reactor=None, executor=self.E)

        C = FacetSuperCollector(I._indexAndTaxonomy.taxoReader, I._facetsConfig)
        Q = MatchAllDocsQuery()
        I.search(Q, None, C)
        td = C.getTopChildren(10, "facet1", [])
        self.assertEquals([
                ('value90', 100),
                ('value91', 100),
                ('value92', 100),
                ('value93', 100),
                ('value94', 100),
                ('value95', 100),
                ('value96', 100),
                ('value97', 100),
                ('value98', 100),
                ('value99', 100)
            ], [(l.label, l.value.intValue()) for l in td.labelValues])



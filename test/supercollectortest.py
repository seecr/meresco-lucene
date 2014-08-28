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

from seecr.test import SeecrTestCase, CallTrace

from java.util.concurrent import Executors
from lucenetest import document, createDocument
from meresco.lucene.index import Index
from org.apache.lucene.search import MatchAllDocsQuery, Sort
from org.meresco.lucene.search import SuperCollector, SubCollector, TotalHitCountSuperCollector, TopScoreDocSuperCollector, FacetSuperCollector, MultiSuperCollector, TopFieldSuperCollector
from java.util import ArrayList
from meresco.lucene.utils import sortField
from org.apache.lucene.facet import FacetsConfig


class SuperCollectorTest(SeecrTestCase):

    def setUp(self):
        super(SuperCollectorTest, self).setUp()
        self.E = Executors.newFixedThreadPool(2)

    def tearDown(self):
        self.E.shutdown()
        super(SuperCollectorTest, self).tearDown()

    def testCreate(self):
        C = TotalHitCountSuperCollector()
        self.assertTrue(isinstance(C, SuperCollector))
        S = C.subCollector()
        self.assertTrue(isinstance(S, SubCollector))

    def testSearch(self):
        C = TotalHitCountSuperCollector()
        I = Index(path=self.tempdir, reactor=None, executor=self.E)
        Q = MatchAllDocsQuery()
        I.search(Q, None, C)
        self.assertEquals(0, C.getTotalHits())
        I._indexWriter.addDocument(document(name="one", price="2"))
        I.close()
        I = Index(path=self.tempdir, reactor=None, executor=self.E)
        I.search(Q, None, C)
        self.assertEquals(1, C.getTotalHits())

    def testTopScoreDocSuperCollector(self):
        C = TopScoreDocSuperCollector(10, True)
        self.assertTrue(isinstance(C, SuperCollector))
        S = C.subCollector()
        self.assertTrue(isinstance(S, SubCollector))
        self.assertFalse(S.acceptsDocsOutOfOrder())
        C = TopScoreDocSuperCollector(10, False)
        S = C.subCollector()
        self.assertTrue(S.acceptsDocsOutOfOrder())

    def testSearchTopDocs(self):
        I = Index(path=self.tempdir, reactor=None, executor=self.E)
        I._indexWriter.addDocument(document(name="one", price="aap noot mies"))
        I._indexWriter.addDocument(document(name="two", price="aap vuur boom"))
        I._indexWriter.addDocument(document(name="three", price="noot boom mies"))
        I.close()
        I = Index(path=self.tempdir, reactor=None, executor=self.E)
        C = TopScoreDocSuperCollector(2, True)
        Q = MatchAllDocsQuery()
        I.search(Q, None, C)
        td = C.topDocs(0)
        self.assertEquals(3, C.getTotalHits())
        self.assertEquals(3, td.totalHits)
        self.assertEquals(2, len(td.scoreDocs))

    def testSearchTopDocsWithStart(self):
        I = Index(path=self.tempdir, reactor=None, executor=self.E)
        I._indexWriter.addDocument(document(name="one", price="aap noot mies"))
        I._indexWriter.addDocument(document(name="two", price="aap vuur boom"))
        I._indexWriter.addDocument(document(name="three", price="noot boom mies"))
        I.close()
        I = Index(path=self.tempdir, reactor=None, executor=self.E)
        C = TopScoreDocSuperCollector(2, True)
        Q = MatchAllDocsQuery()
        I.search(Q, None, C)
        td = C.topDocs(1)
        self.assertEquals(3, C.getTotalHits())
        self.assertEquals(3, td.totalHits)
        self.assertEquals(1, len(td.scoreDocs))
        self.assertEquals([1], [sd.score for sd in td.scoreDocs])

    def testFacetSuperCollector(self):
        I = Index(path=self.tempdir, reactor=None, executor=self.E, facetsConfig=FacetsConfig())
        for i in xrange(10000):
            document1 = createDocument(fields=[("field1", str(i)), ("field2", str(i)*1000)], facets=[("facet1", "value%s" % (i % 100))])
            document1 = I._facetsConfig.build(I._taxoWriter, document1)
            I._indexWriter.addDocument(document1)
        I.close()
        I = Index(path=self.tempdir, reactor=None, executor=self.E, facetsConfig=FacetsConfig())

        C = FacetSuperCollector(I._indexAndTaxonomy.taxoReader, I._facetsConfig, I._ordinalsReader)
        Q = MatchAllDocsQuery()
        I.search(Q, None, C)
        tc = C.getTopChildren(10, "facet1", [])
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
            ], [(l.label, l.value.intValue()) for l in tc.labelValues])

    def testFacetAndTopsMultiCollector(self):
        I = Index(path=self.tempdir, commitCount=1, reactor=CallTrace(), executor=self.E, facetsConfig=FacetsConfig())
        for i in xrange(99):
            document1 = createDocument(fields=[("field1", str(i)), ("field2", str(i)*1000)], facets=[("facet1", "value%s" % (i % 10))])
            document1 = I._facetsConfig.build(I._taxoWriter, document1)
            I._indexWriter.addDocument(document1)
            I.commit()
        I.close()
        I = Index(path=self.tempdir, reactor=None, executor=self.E, facetsConfig=FacetsConfig())

        f = FacetSuperCollector(I._indexAndTaxonomy.taxoReader, I._facetsConfig, I._ordinalsReader)
        t = TopScoreDocSuperCollector(10, True)
        collectors = ArrayList().of_(SuperCollector)
        collectors.add(t)
        collectors.add(f)
        C = MultiSuperCollector(collectors)
        Q = MatchAllDocsQuery()
        I.search(Q, None, C)

        self.assertEquals(99, t.topDocs(0).totalHits)
        self.assertEquals(10, len(t.topDocs(0).scoreDocs))
        tc = f.getTopChildren(10, "facet1", [])

        self.assertEquals([
                ('value0', 10),
                ('value1', 10),
                ('value2', 10),
                ('value3', 10),
                ('value4', 10),
                ('value5', 10),
                ('value6', 10),
                ('value7', 10),
                ('value8', 10),
                ('value9', 9)
            ], [(l.label, l.value.intValue()) for l in tc.labelValues])

    def testSearchTopField(self):
        I = Index(path=self.tempdir, executor=self.E, commitCount=1, reactor=CallTrace())
        I._indexWriter.addDocument(document(__id__='1', name="one", price="aap noot mies"))
        I.commit()
        I._indexWriter.addDocument(document(__id__='2', name="two", price="aap vuur boom"))
        I.commit()
        I._indexWriter.addDocument(document(__id__='3', name="three", price="noot boom mies"))
        I.commit()
        I.close()
        I = Index(path=self.tempdir, reactor=None, executor=self.E)
        sort = Sort(sortField(fieldname="name", sortDescending=True))
        C = TopFieldSuperCollector(sort, 2, True, False, True)
        Q = MatchAllDocsQuery()
        I.search(Q, None, C)
        td = C.topDocs(0)
        self.assertEquals(3, C.getTotalHits())
        self.assertEquals(3, td.totalHits)
        self.assertEquals(2, len(td.scoreDocs))
        self.assertEquals(['2', '3'], [I.getDocument(s.doc).get("__id__") for s in td.scoreDocs])

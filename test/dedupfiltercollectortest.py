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

from org.apache.lucene.search import TopScoreDocCollector, MatchAllDocsQuery
from org.apache.lucene.document import Document, NumericDocValuesField
from org.meresco.lucene import DeDupFilterCollector

from seecr.test import SeecrTestCase, CallTrace
from weightless.core import consume

from meresco.lucene import Lucene
from meresco.lucene.utils import IDFIELD


class DeDupFilterCollectorTest(SeecrTestCase):
    def setUp(self):
        super(DeDupFilterCollectorTest, self).setUp()
        self._reactor = CallTrace('reactor')
        self.lucene = Lucene(self.tempdir, commitCount=1, reactor=self._reactor)

    def tearDown(self):
        self.lucene.close()
        super(DeDupFilterCollectorTest, self).tearDown()

    def testCollectorTransparentlyDelegatesToNextCollector(self):
        self._addDocument("urn:1", 2)
        tc = TopScoreDocCollector.create(100, True)
        c = DeDupFilterCollector("__isformatof__", tc)
        consume(self.lucene.search(query=MatchAllDocsQuery(), collector=c))
        self.assertEquals(1, tc.topDocs().totalHits)

    def _addDocument(self, identifier, isformatof):
        doc = Document()
        if isformatof:
            doc.add(NumericDocValuesField("__isformatof__", long(isformatof)))
        consume(self.lucene.addDocument(identifier, doc))
        self.lucene.commit()  # Explicitly, not required: since commitCount=1.

    def testCollectorFiltersTwoSimilar(self):
        self._addDocument("urn:1", 2)
        self._addDocument("urn:2", 2)
        tc = TopScoreDocCollector.create(100, True)
        c = DeDupFilterCollector("__isformatof__", tc)
        consume(self.lucene.search(query=MatchAllDocsQuery(), collector=c))
        topDocsResult = tc.topDocs()
        self.assertEquals(1, topDocsResult.totalHits)
        self.assertEquals(1, len(topDocsResult.scoreDocs))

        docId = topDocsResult.scoreDocs[0].doc
        key = c.keyForDocId(docId)
        self.assertEquals(0, key.docId)
        self.assertEquals(2, key.count)

    def testCollectorFiltersTwoTimesTwoSimilarOneNot(self):
        self._addDocument("urn:1", 1)
        self._addDocument("urn:2", 3)
        self._addDocument("urn:3", 50)
        self._addDocument("urn:4", 3)
        self._addDocument("urn:5", 1)
        tc = TopScoreDocCollector.create(100, True)
        c = DeDupFilterCollector("__isformatof__", tc)
        consume(self.lucene.search(query=MatchAllDocsQuery(), collector=c))
        topDocsResult = tc.topDocs()
        self.assertEquals(3, topDocsResult.totalHits)
        self.assertEquals(3, len(topDocsResult.scoreDocs))

        docIds = [s.doc for s in topDocsResult.scoreDocs]
        index = self.lucene._index
        identifierAndDocIds = [(d, index.getDocument(d).get(IDFIELD)) for d in docIds]
        self.assertEquals([(0, "urn:1"), (1, "urn:2"), (2, "urn:3")], identifierAndDocIds)
        self.assertEquals(2, c.keyForDocId(docIds[0]).count)
        self.assertEquals(2, c.keyForDocId(docIds[1]).count)
        self.assertEquals(1, c.keyForDocId(docIds[2]).count)

    def testSilentyYieldsWrongResultWhenFieldNameDoesNotMatch(self):
        self._addDocument("urn:1", 2)
        tc = TopScoreDocCollector.create(100, True)
        c = DeDupFilterCollector("__wrong_field__", tc)
        consume(self.lucene.search(query=MatchAllDocsQuery(), collector=c))
        self.assertEquals(1, tc.topDocs().totalHits)

    def testShouldAddResultsWithoutIsFormatOf(self):
        self._addDocument("urn:1", 2)
        self._addDocument("urn:2", None)
        self._addDocument("urn:3", 2)
        self._addDocument("urn:4", None)
        self._addDocument("urn:5", None)
        self._addDocument("urn:6", None)
        self._addDocument("urn:7", None)
        self._addDocument("urn:8", None)
        self._addDocument("urn:9", None)
        self._addDocument("urn:A", None)
        self._addDocument("urn:B", None) # trigger a merge
        tc = TopScoreDocCollector.create(100, True)
        c = DeDupFilterCollector("__isformatof__", tc)
        consume(self.lucene.search(query=MatchAllDocsQuery(), collector=c))
        self.assertEquals(10, tc.topDocs().totalHits)


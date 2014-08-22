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

from org.apache.lucene.search import MatchAllDocsQuery
from org.apache.lucene.document import Document, NumericDocValuesField
from org.meresco.lucene.search import DeDupFilterSuperCollector, TopScoreDocSuperCollector

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
        tc = TopScoreDocSuperCollector(100, True)
        c = DeDupFilterSuperCollector("__isformatof__", "__sort__", tc)
        self.lucene.search(query=MatchAllDocsQuery(), collector=c)
        self.assertEquals(1, tc.topDocs(0).totalHits)

    def _addDocument(self, identifier, isformatof, sort=None):
        doc = Document()
        if isformatof:
            doc.add(NumericDocValuesField("__isformatof__", long(isformatof)))
        if sort:
            doc.add(NumericDocValuesField("__sort__", long(sort)))
        consume(self.lucene.addDocument(identifier, doc))
        self.lucene.commit()  # Explicitly, not required: since commitCount=1.

    def testCollectorFiltersTwoSimilar(self):
        self._addDocument("urn:1", 2, 1)
        self._addDocument("urn:2", 2, 2)
        tc = TopScoreDocSuperCollector(100, True)
        c = DeDupFilterSuperCollector("__isformatof__", "__sort__", tc)
        self.lucene.search(query=MatchAllDocsQuery(), collector=c)
        topDocsResult = tc.topDocs(0)
        self.assertEquals(1, topDocsResult.totalHits)
        self.assertEquals(1, len(topDocsResult.scoreDocs))

        docId = topDocsResult.scoreDocs[0].doc
        key = c.keyForDocId(docId)
        identifier = self.lucene._index.getDocument(key.getDocId()).get(IDFIELD)
        self.assertEquals('urn:2', identifier)
        self.assertEquals(2, key.count)

    def testCollectorFiltersTwoTimesTwoSimilarOneNot(self):
        self._addDocument("urn:1", 1, 1)
        self._addDocument("urn:2", 3, 2)
        self._addDocument("urn:3", 50, 3)
        self._addDocument("urn:4", 3, 4)
        self._addDocument("urn:5", 1, 5)
        tc = TopScoreDocSuperCollector(100, True)
        c = DeDupFilterSuperCollector("__isformatof__", "__sort__", tc)
        self.lucene.search(query=MatchAllDocsQuery(), collector=c)
        topDocsResult = tc.topDocs(0)
        self.assertEquals(3, topDocsResult.totalHits)
        self.assertEquals(3, len(topDocsResult.scoreDocs))

        docIds = [s.doc for s in topDocsResult.scoreDocs]
        realKeys = [c.keyForDocId(docId) for docId in docIds]
        index = self.lucene._index
        identifierAndDocIds = [(k.docId, index.getDocument(k.docId).get(IDFIELD), k.count) for k in realKeys]
        self.assertEquals(set([(2, "urn:3", 1), (3, "urn:4", 2), (4, "urn:5", 2)]), set(identifierAndDocIds))

    def testSilentyYieldsWrongResultWhenFieldNameDoesNotMatch(self):
        self._addDocument("urn:1", 2)
        tc = TopScoreDocSuperCollector(100, True)
        c = DeDupFilterSuperCollector("__wrong_field__", "__sort__", tc)
        self.lucene.search(query=MatchAllDocsQuery(), collector=c)
        self.assertEquals(1, tc.topDocs(0).totalHits)

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
        tc = TopScoreDocSuperCollector(100, True)
        c = DeDupFilterSuperCollector("__isformatof__", "__sort__", tc)
        self.lucene.search(query=MatchAllDocsQuery(), collector=c)
        self.assertEquals(10, tc.topDocs(0).totalHits)


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

from seecr.test import SeecrTestCase, CallTrace
from meresco.lucene import LuceneSettings, Lucene
from org.apache.lucene.document import NumericDocValuesField, Document
from weightless.core import consume
from org.meresco.lucene.search import TopScoreDocSuperCollector, GroupSuperCollector
from org.apache.lucene.search import MatchAllDocsQuery
from meresco.lucene.fieldregistry import IDFIELD

class GroupCollectorTest(SeecrTestCase):
    def setUp(self):
        super(GroupCollectorTest, self).setUp()
        self._reactor = CallTrace('reactor')
        settings = LuceneSettings(commitCount=1, verbose=False)
        self.lucene = Lucene(self.tempdir, reactor=self._reactor, settings=settings)

    def tearDown(self):
        self.lucene.close()
        super(GroupCollectorTest, self).tearDown()

    def testOne(self):
        self._addDocument('id:0', isformatof=42)
        self._addDocument('id:1', isformatof=42)
        self._addDocument('id:2', isformatof=17)
        tc = TopScoreDocSuperCollector(100, True)
        c = GroupSuperCollector("__isformatof__", tc)
        self.lucene.search(query=MatchAllDocsQuery(), collector=c)
        self.assertEquals(3, tc.getTotalHits())
        idFields = dict((self.lucene._index.getDocument(scoreDoc.doc).get(IDFIELD), scoreDoc.doc) for scoreDoc in tc.topDocs(0).scoreDocs)

        self.assertEquals(2, len(list(c.group(idFields['id:0']))))
        self.assertEquals(2, len(list(c.group(idFields['id:1']))))
        self.assertEquals(1, len(list(c.group(idFields['id:2']))))


    def _addDocument(self, identifier, isformatof=None):
        doc = Document()
        if isformatof is not None:
            doc.add(NumericDocValuesField("__isformatof__", long(isformatof)))
        consume(self.lucene.addDocument(identifier=identifier, document=doc))
        self.lucene.commit()  # Explicitly, not required: since commitCount=1.
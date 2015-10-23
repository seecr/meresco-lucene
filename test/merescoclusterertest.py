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

from weightless.core import consume
from org.apache.lucene.document import Document
from meresco.lucene.fieldregistry import FieldRegistry
from org.meresco.lucene.search import MerescoClusterer

from lucenetestcase import LuceneTestCase


class MerescoClustererTest(LuceneTestCase):
    def setUp(self):
        LuceneTestCase.setUp(self)
        factory = FieldRegistry(termVectorFields=['termvector.field'])
        for i in range(5):
            doc = Document()
            doc.add(factory.createField("termvector.field", "aap noot noot noot vuur"))
            consume(self.lucene.addDocument(identifier="id:%s" % i , document=doc))

        for i in range(5, 10):
            doc = Document()
            doc.add(factory.createField("termvector.field", "something else"))
            consume(self.lucene.addDocument(identifier="id:%s" % i, document=doc))

        for i in range(10, 15):
            doc = Document()
            doc.add(factory.createField("termvector.field", "iets anders"))
            consume(self.lucene.addDocument(identifier="id:%s" % i, document=doc))
        self.lucene.commit()
        reader = self.lucene._index._indexAndTaxonomy.searcher.getIndexReader()
        self.collector = MerescoClusterer(reader, 0.5)

    def tearDown(self):
        del self.collector
        LuceneTestCase.tearDown(self)

    def testClusterOnTermVectors(self):
        self.collector.registerField("termvector.field", 1.0, None)
        for i in range(15):
            self.collector.collect(i)
        self.collector.finish()

        cluster1 = self.collector.cluster(0)
        self.assertEqual(5, len(list(cluster1.topDocs)))
        self.assertEqual(['else', 'something'], list([t.term for t in cluster1.topTerms]))

        cluster2 = self.collector.cluster(5)
        self.assertEqual(5, len(list(cluster2.topDocs)))
        self.assertEqual(['noot', 'aap', 'vuur'], list([t.term for t in cluster2.topTerms]))

        cluster3 = self.collector.cluster(10)
        self.assertEqual(5, len(list(cluster3.topDocs)))
        self.assertEqual(['anders', 'iets'], list([t.term for t in cluster3.topTerms]))

        self.assertNotEqual(cluster1.topDocs, cluster2.topDocs)
        self.assertNotEqual(cluster1.topDocs, cluster3.topDocs)

    def testClusteringWithFieldFilter(self):
        self.collector.registerField("termvector.field", 1.0, 'noot')
        for i in range(15):
            self.collector.collect(i)
        self.collector.finish()

        cluster1 = self.collector.cluster(0)
        self.assertEquals(None, cluster1)

        cluster2 = self.collector.cluster(5)
        self.assertEqual(5, len(list(cluster2.topDocs)))
        self.assertEqual(['noot', 'aap', 'vuur'], list([t.term for t in cluster2.topTerms]))

        cluster3 = self.collector.cluster(10)
        self.assertEquals(None, cluster3)

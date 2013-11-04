
from org.apache.lucene.search import TopScoreDocCollector, MatchAllDocsQuery
from org.apache.lucene.document import Document, Field, LongField
from org.meresco.lucene import DeDupFilterCollector
from seecr.test import SeecrTestCase, CallTrace
from weightless.core import consume

from meresco.lucene import Lucene

class DeDupFilterCollectorTest(SeecrTestCase):

    def setUp(self):
        super(DeDupFilterCollectorTest, self).setUp()
        self.lucene = Lucene(self.tempdir, commitCount=1, reactor=CallTrace())

    def tearDown(self):
        self.lucene.finish()
        super(DeDupFilterCollectorTest, self).tearDown()

    def testCreateCollector(self):
        doc1 = Document()
        doc1.add(LongField("__isformatof__", 2L, Field.Store.YES))
        consume(self.lucene.addDocument("urn:1", doc1))
        tc = TopScoreDocCollector.create(1, True)
        c = DeDupFilterCollector("__isformatof__", tc)
        self.lucene.search(MatchAllDocsQuery(), c)
        self.assertTrue(c.result)

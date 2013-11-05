
from org.apache.lucene.search import TopScoreDocCollector, MatchAllDocsQuery
from org.apache.lucene.document import Document, Field, NumericDocValuesField
from org.meresco.lucene import DeDupFilterCollector
from seecr.test import SeecrTestCase, CallTrace
from weightless.core import consume

from meresco.lucene import Lucene

class DeDupFilterCollectorTest(SeecrTestCase):

    def setUp(self):
        super(DeDupFilterCollectorTest, self).setUp()
        self.lucene = Lucene(self.tempdir, commitCount=1, reactor=CallTrace())

    def tearDown(self):
        self.lucene.close()
        super(DeDupFilterCollectorTest, self).tearDown()

    def testCollectorTransparentlyDelegatesToNextCollector(self):
        self.addDocument("urn:1", 2)
        tc = TopScoreDocCollector.create(100, True)
        c = DeDupFilterCollector("__isformatof__", tc)
        consume(self.lucene.search(query=MatchAllDocsQuery(), collector=c))
        self.assertEquals(1, tc.topDocs().totalHits)

    def addDocument(self, identifier, isformatof):
        doc = Document()
        if isformatof:
            doc.add(NumericDocValuesField("__isformatof__", long(isformatof)))
        consume(self.lucene.addDocument(identifier, doc))
        self.lucene.commit()

    def testCollectorFilters(self):
        self.addDocument("urn:1", 2)
        self.addDocument("urn:2", 2)
        tc = TopScoreDocCollector.create(100, True)
        c = DeDupFilterCollector("__isformatof__", tc)
        consume(self.lucene.search(query=MatchAllDocsQuery(), collector=c))
        self.assertEquals(1, tc.topDocs().totalHits)

    def testSilentyYieldsWrongResultWhenFieldNameDoesNotMatch(self):
        self.addDocument("urn:1", 2)
        tc = TopScoreDocCollector.create(100, True)
        c = DeDupFilterCollector("__wrong_field__", tc)
        consume(self.lucene.search(query=MatchAllDocsQuery(), collector=c))
        self.assertEquals(1, tc.topDocs().totalHits)
       
    def testShouldAddResultsWithoutIsFormatOf(self):
        self.addDocument("urn:1", 2)
        self.addDocument("urn:2", None)
        self.addDocument("urn:3", 2)
        self.addDocument("urn:4", None)
        self.addDocument("urn:5", None)
        self.addDocument("urn:6", None)
        self.addDocument("urn:7", None)
        self.addDocument("urn:8", None)
        self.addDocument("urn:9", None)
        self.addDocument("urn:A", None)
        self.addDocument("urn:B", None) # trigger a merge
        tc = TopScoreDocCollector.create(100, True)
        c = DeDupFilterCollector("__isformatof__", tc)
        consume(self.lucene.search(query=MatchAllDocsQuery(), collector=c))
        self.assertEquals(10, tc.topDocs().totalHits)


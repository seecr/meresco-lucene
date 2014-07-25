from seecr.test import SeecrTestCase
from java.util.concurrent import Executors
from org.apache.lucene.search import MatchAllDocsQuery
from org.meresco.lucene.search import SuperCollector, SubCollector, CountingSuperCollector, TopDocsSuperCollector
from meresco.lucene.index import Index

from lucenetest import document

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


from seecr.test import SeecrTestCase
from org.meresco.lucene.search import SuperCollector, SubCollector, CountingSuperCollector

from lucenetest import document
from meresco.lucene.index import Index

class SuperCollectorTest(SeecrTestCase):
    def testCreate(self):
        C = CountingSuperCollector()
        self.assertTrue(isinstance(C, SuperCollector))
        S = C.subCollector(None)
        self.assertTrue(isinstance(S, SubCollector))

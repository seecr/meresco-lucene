import unittest

from org.apache.lucene.analysis import Analyzer
from org.apache.lucene.analysis.standard import StandardAnalyzer

from org.meresco.lucene import DelegatingAnalyzer

class DelegatingAnalyzerTest(unittest.TestCase):

    def test_analyzer(self):
        d = StandardAnalyzer()
        a = DelegatingAnalyzer(d, 10)

        assert 10 == a.getPositionIncrementGap("field1")

        self.fail()

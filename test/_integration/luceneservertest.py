## begin license ##
#
# "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
#
# Copyright (C) 2015-2016 Koninklijke Bibliotheek (KB) http://www.kb.nl
# Copyright (C) 2015-2016 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2016 Stichting Kennisnet http://www.kennisnet.nl
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

from simplejson import loads

from seecr.test import IntegrationTestCase
from seecr.test.utils import postRequest, getRequest

from meresco.components.http.utils import CRLF
from meresco.components.json import JsonList, JsonDict

from meresco.lucene import ComposedQuery, readFixedBitSet


class LuceneServerTest(IntegrationTestCase):
    def setUp(self):
        IntegrationTestCase.setUp(self)
        self._path = "/default"
        header, body = postRequest(self.luceneServerPort, self._path + '/settings/', data=JsonDict(commitCount=1).dumps())
        self.assertTrue("200 OK" in header.upper(), header)

    def testAddAndQueryDocument(self):
        data = JsonList([
                {"type": "TextField", "name": "fieldname", "value": "value"}
            ]).dumps()
        header, body = postRequest(self.luceneServerPort, self._path + '/update/?identifier=id1', data=data)
        self.assertTrue("200 OK" in header.upper(), header)

        header, body = postRequest(self.luceneServerPort, self._path + '/query/', data=JsonDict(query=dict(type="MatchAllDocsQuery")).dumps(), parse=False)
        self.assertTrue("200 OK" in header.upper(), header)
        response = loads(body)
        self.assertEqual(1, response['total'])
        self.assertEqual([{'id': 'id1', 'score': 1.0}], response['hits'])

        header, body = postRequest(self.luceneServerPort, self._path + '/query/', data=JsonDict(query=dict(type="TermQuery", term=dict(field="fieldname", value="value"))).dumps(), parse=False)
        self.assertTrue("200 OK" in header.upper(), header)
        response = loads(body)
        self.assertEqual(1, response['total'])
        self.assertTrue("queryTime" in response)
        self.assertTrue("times" in response)
        self.assertEqual([{'id': 'id1', 'score': 0.28768208622932434}], response['hits'])

    def testFacets(self):
        data = JsonList([
                {"type": "TextField", "name": "fieldname", "value": "value"},
                {"type": "FacetField", "name": "fieldname", "path": ["value"]}
            ]).dumps()
        header, body = postRequest(self.luceneServerPort, self._path + '/update/?identifier=id1', data=data)
        self.assertTrue("200 OK" in header.upper(), header)

        header, body = postRequest(self.luceneServerPort, self._path + '/query/', data=JsonDict(query=dict(type="MatchAllDocsQuery"), facets=[{"fieldname": "fieldname", "maxTerms": 10}]).dumps(), parse=False)
        self.assertTrue("200 OK" in header.upper(), header)
        jsonResponse = loads(body)
        self.assertEqual(1, jsonResponse['total'])
        self.assertEqual([{'path': [], 'fieldname': 'fieldname', 'terms': [{'count': 1, 'term': 'value'}]}], jsonResponse['drilldownData'])
        self.assertTrue("facetTime" in jsonResponse["times"])

    def testPrefixSearch(self):
        data = JsonList([
                {"type": "TextField", "name": "prefixField", "value": "value0"},
                {"type": "TextField", "name": "prefixField", "value": "value1"},
                {"type": "TextField", "name": "prefixField", "value": "value2"},
            ]).dumps()
        header, body = postRequest(self.luceneServerPort, self._path + '/update/?identifier=id1', data=data)
        self.assertTrue("200 OK" in header.upper(), header)

        header, body = postRequest(self.luceneServerPort, self._path + '/prefixSearch/?fieldname=prefixField&prefix=val', parse=False)
        self.assertEqual([['value0', 1], ['value1', 1], ['value2', 1]], loads(body))

    def testSuggestionRequest(self):
        data = JsonList([
                {"type": "TextField", "name": "field", "value": "value"},
            ]).dumps()
        header, body = postRequest(self.luceneServerPort, self._path + '/update/?identifier=id1', data=data)
        self.assertTrue("200 OK" in header.upper(), header)

        header, body = postRequest(self.luceneServerPort, self._path + '/query/', parse=False, data=JsonDict(query=dict(type="MatchAllDocsQuery"), suggestionRequest=dict(field="field", count=1, suggests=['valeu'])).dumps())
        jsonResponse = loads(body)
        self.assertEqual({'valeu': ['value']}, jsonResponse["suggestions"])
        self.assertTrue("suggestionTime" in jsonResponse["times"])

    def testSettings(self):
        header, body = getRequest(self.luceneServerPort, self._path + '/settings/', parse=False)
        self.assertTrue("200 OK" in header.upper(), header)
        self.assertEqual({
                'similarity': 'BM25(k1=1.2,b=0.75)',
                'clustering': {
                    'clusterMoreRecords': 100,
                    'strategies': [{
                        'clusteringEps': 0.4,
                        'clusteringMinPoints': 1,
                    }]
                },
                'commitCount': 1,
                'commitTimeout': 10,
                'lruTaxonomyWriterCacheSize': 4000,
                'maxMergeAtOnce': 2,
                'numberOfConcurrentTasks': 6,
                'segmentsPerTier': 8.0
            }, loads(body))

    def testNumerate(self):
        header, body = postRequest(self.luceneServerPort, '/numerate/', data='id0', parse=False)
        self.assertTrue("200 OK" in header.upper(), header)
        header, body2 = postRequest(self.luceneServerPort, '/numerate/', data='id0', parse=False)
        self.assertTrue("200 OK" in header.upper(), header)
        self.assertEquals(body2, body)
        header, body3 = postRequest(self.luceneServerPort, '/numerate/', data='id1', parse=False)
        self.assertNotEquals(body3, body)

    def testCommit(self):
        header, body = postRequest(self.luceneServerPort, self._path + '/settings/', data=JsonDict(commitCount=10).dumps())
        self.assertTrue("200 OK" in header.upper(), header)

        try:
            data = JsonList([
                    {"type": "TextField", "name": "fieldname", "value": "value"}
                ]).dumps()
            header, body = postRequest(self.luceneServerPort, self._path + '/update/?identifier=idCommit', data=data)
            self.assertTrue("200 OK" in header.upper(), header)

            header, body = postRequest(self.luceneServerPort, self._path + '/query/', parse=False, data=JsonDict(query=dict(type="TermQuery", term=dict(field="__id__", value="idCommit"))).dumps())
            response = loads(body)
            self.assertEqual(0, response['total'])

            header, body = postRequest(self.luceneServerPort, '/commit/', parse=False)
            self.assertTrue("200 OK" in header.upper(), header)

            header, body = postRequest(self.luceneServerPort, self._path + '/query/', parse=False, data=JsonDict(query=dict(type="TermQuery", term=dict(field="__id__", value="idCommit"))).dumps())
            response = loads(body)
            self.assertEqual(1, response['total'])
        finally:
            header, body = postRequest(self.luceneServerPort, self._path + '/delete/?identifier=idCommit', data=data)
            self.assertTrue("200 OK" in header.upper(), header)


    def testExportKeys(self):
        composedQuery = ComposedQuery('main')
        composedQuery.setCoreQuery('main', query=dict(type="MatchAllDocsQuery"))
        header, body = postRequest(self.luceneServerPort, '/exportkeys/?exportKey=__key__.field', data=JsonDict(composedQuery.asDict()).dumps(), parse=False)
        self.assertTrue("200 OK" in header.upper(), header + 2 * CRLF + body)
        bitSet = readFixedBitSet(body)
        for i in xrange(0, 102):
            isSet = bitSet.get(i)
            if 2 < i < 101:
                self.assertTrue(isSet, i)
            else:
                self.assertFalse(isSet, i)

    def testSimilarDocs(self):
        header, body = postRequest(self.luceneServerPort, self._path + '/similarDocuments/?identifier=id1', data="", parse=False)
        self.assertTrue("200 OK" in header.upper(), header + 2 * CRLF + body)
        self.assertEqual({"total":0,"queryTime":0,"hits":[]}, loads(body))

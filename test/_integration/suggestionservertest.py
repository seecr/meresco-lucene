## begin license ##
#
# "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
#
# Copyright (C) 2016 Koninklijke Bibliotheek (KB) http://www.kb.nl
# Copyright (C) 2016 Seecr (Seek You Too B.V.) http://seecr.nl
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


from seecr.test import IntegrationTestCase
from seecr.test.utils import postRequest, getRequest
from simplejson import loads
from org.apache.lucene.util import OpenBitSet
from StringIO import StringIO
from struct import pack


class SuggestionServerTest(IntegrationTestCase):
    def testAdd(self):
        data = """{
            "key": 1,
            "values": ["harry"], "types": ["uri:book"], "creators": ["rowling"]
        }"""
        try:
            header, body = postRequest(self.suggestionServerPort, '/add?identifier=id1', data=data, parse=False)
            self.assertTrue("200 OK" in header.upper(), header + body)

            header, body = postRequest(self.suggestionServerPort, '/commit', data=None, parse=False)

            header, body = getRequest(self.suggestionServerPort, '/totalRecords', parse=False)
            self.assertTrue("200 OK" in header.upper(), header + body)
            self.assertEqual("1", body)

            header, body = getRequest(self.suggestionServerPort, '/totalSuggestions', parse=False)
            self.assertTrue("200 OK" in header.upper(), header + body)
            self.assertEqual("0", body)
        finally:
            postRequest(self.suggestionServerPort, '/delete?identifier=id1', data=None, parse=False)
            postRequest(self.suggestionServerPort, '/commit', data=None, parse=False)

    def testAddNulls(self):
        data = """{
            "key": 1,
            "values": ["harry"], "types": [null], "creators": [null]
        }"""
        try:
            header, body = postRequest(self.suggestionServerPort, '/add?identifier=id1', data=data, parse=False)
            self.assertTrue("200 OK" in header.upper(), header + body)
        finally:
            postRequest(self.suggestionServerPort, '/delete?identifier=id1', data=None, parse=False)
            postRequest(self.suggestionServerPort, '/commit', data=None, parse=False)

    def testDelete(self):
        header, body = postRequest(self.suggestionServerPort, '/delete?identifier=id1', data=None, parse=False)
        self.assertTrue("200 OK" in header.upper(), header + body)

    def testCreate(self):
        header, body = postRequest(self.suggestionServerPort, '/createSuggestionNGramIndex?wait=True', data=None, parse=False)
        self.assertTrue("200 OK" in header.upper(), header + body)

    def testCommit(self):
        header, body = postRequest(self.suggestionServerPort, '/commit', parse=False)
        self.assertTrue("200 OK" in header.upper(), header)

    def testSuggest(self):
        data = """{
            "value": "ha",
            "trigram": false,
            "filters": [],
            "keySetName": null
        }"""
        header, body = postRequest(self.suggestionServerPort, '/suggest', data=data, parse=False)
        self.assertTrue("200 OK" in header.upper(), header + body)
        self.assertTrue("Content-Type: application/json" in header, header)
        self.assertEqual("[]", body)

    def testAddCreateAndSuggest(self):
        data = """{
            "key": 1,
            "values": ["harry"], "types": ["uri:book"], "creators": ["rowling"]
        }"""
        try:
            postRequest(self.suggestionServerPort, '/add?identifier=id1', data=data, parse=False)
            postRequest(self.suggestionServerPort, '/createSuggestionNGramIndex?wait=True', data=None, parse=False)
            data = """{
                "value": "ha",
                "trigram": false,
                "filters": [],
                "keySetName": null
            }"""
            header, body = postRequest(self.suggestionServerPort, '/suggest', data=data, parse=False)
            self.assertTrue("200 OK" in header.upper(), header + body)
            self.assertEqual([
                {"suggestion": "harry", "type": "uri:book", "creator": 'rowling', "score": 0.5348455309867859},
            ], loads(body))
        finally:
            postRequest(self.suggestionServerPort, '/delete?identifier=id1', data=None, parse=False)
            postRequest(self.suggestionServerPort, '/commit', data=None, parse=False)

    def testAutocompleteWithSuggestionIndexComponent(self):
        data = """{
            "key": 1,
            "values": ["harry"], "types": ["uri:book"], "creators": ["rowling"]
        }"""
        postRequest(self.suggestionServerPort, '/add?identifier=id1', data=data, parse=False)

        data = """{
            "key": 2,
            "values": ["hallo"], "types": ["uri:ebook"], "creators": [null]
        }"""
        postRequest(self.suggestionServerPort, '/add?identifier=id2', data=data, parse=False)
        try:
            postRequest(self.suggestionServerPort, '/createSuggestionNGramIndex?wait=True', data=None, parse=False)
            header, body = getRequest(port=self.httpPort, path='/suggestion', arguments={'value': 'ha'}, parse=False)
            self.assertEqual(["ha", ["harry", "hallo"]], loads(body))

            header, body = getRequest(port=self.httpPort, path='/suggestion', arguments={'value': 'ha', "filter": "type=uri:book"}, parse=False)
            self.assertEqual(["ha", ["harry"]], loads(body))
        finally:
            postRequest(self.suggestionServerPort, '/delete?identifier=id1', data=None, parse=False)
            postRequest(self.suggestionServerPort, '/delete?identifier=id2', data=None, parse=False)
            postRequest(self.suggestionServerPort, '/commit', data=None, parse=False)

    def testIndexingState(self):
        header, body = getRequest(self.suggestionServerPort, '/indexingState', parse=False)
        self.assertTrue("200 OK" in header.upper(), header + body)
        self.assertTrue("Content-Type: application/json" in header, header)
        self.assertEqual("{}", body)

        postRequest(self.suggestionServerPort, '/createSuggestionNGramIndex', data=None, parse=False)
        header, body = getRequest(self.suggestionServerPort, '/indexingState', parse=False)
        self.assertTrue("200 OK" in header.upper(), header + body)
        self.assertTrue("Content-Type: application/json" in header, header)
        self.assertNotEqual("{}", body)
        self.assertTrue("started" in loads(body), body)
        self.assertTrue("count" in loads(body), body)

    def testRegisterKeySet(self):
        keySet = OpenBitSet()
        keySet.set(2L)

        header, body = postRequest(self.suggestionServerPort, '/registerFilterKeySet?name=test', data=openBitSetAsBytes(keySet), parse=False)
        self.assertTrue("200 OK" in header.upper(), header + body)


def openBitSetAsBytes(bitSet):
    s = StringIO()
    s.write(pack('>i', bitSet.getNumWords()))
    bits = bitSet.getBits()
    s.write(pack('>i', len(bits)))
    for i in xrange(0, len(bits)):
        s.write(pack('>q', bits[i]))
    return s.getvalue()

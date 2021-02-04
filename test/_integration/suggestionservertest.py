## begin license ##
#
# "Meresco Lucene" is a set of components and tools to integrate Lucene into Meresco
#
# Copyright (C) 2016 Koninklijke Bibliotheek (KB) http://www.kb.nl
# Copyright (C) 2016, 2018-2019, 2021 Seecr (Seek You Too B.V.) https://seecr.nl
# Copyright (C) 2016, 2021 Stichting Kennisnet https://www.kennisnet.nl
# Copyright (C) 2021 Data Archiving and Network Services https://dans.knaw.nl
# Copyright (C) 2021 SURF https://www.surf.nl
# Copyright (C) 2021 The Netherlands Institute for Sound and Vision https://beeldengeluid.nl
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

from io import BytesIO
from struct import pack

from simplejson import loads

from seecr.test import IntegrationTestCase
from seecr.test.utils import postRequest, getRequest

from org.apache.lucene.util import FixedBitSet


class SuggestionServerTest(IntegrationTestCase):
    def testAdd(self):
        data = """{
            "key": 1,
            "values": ["harry"], "types": ["uri:book"], "creators": ["rowling"]
        }"""
        try:
            status, header, body = postRequest(self.suggestionServerPort, '/add?identifier=id1', data=data, parse=False)
            self.assertEqual("200", status)

            status, header, body = postRequest(self.suggestionServerPort, '/commit', data=None, parse=False)

            status, header, body = getRequest(self.suggestionServerPort, '/totalRecords', parse=False)
            self.assertEqual("200", status)
            self.assertEqual(b"1", body)

            status, header, body = getRequest(self.suggestionServerPort, '/totalSuggestions', parse=False)
            self.assertEqual("200", status)
            self.assertEqual(b"0", body)
        finally:
            postRequest(self.suggestionServerPort, '/delete?identifier=id1', data=None, parse=False)
            postRequest(self.suggestionServerPort, '/commit', data=None, parse=False)

    def testAddNulls(self):
        data = """{
            "key": 1,
            "values": ["harry"], "types": [null], "creators": [null]
        }"""
        try:
            status, header, body = postRequest(self.suggestionServerPort, '/add?identifier=id1', data=data, parse=False)
            self.assertEqual("200", status)
        finally:
            postRequest(self.suggestionServerPort, '/delete?identifier=id1', data=None, parse=False)
            postRequest(self.suggestionServerPort, '/commit', data=None, parse=False)

    def testDelete(self):
        status, header, body = postRequest(self.suggestionServerPort, '/delete?identifier=id1', data=None, parse=False)
        self.assertEqual("200", status)

    def testCreate(self):
        status, header, body = postRequest(self.suggestionServerPort, '/createSuggestionNGramIndex?wait=True', data=None, parse=False)
        self.assertEqual("200", status)

    def testCommit(self):
        status, header, body = postRequest(self.suggestionServerPort, '/commit', parse=False)
        self.assertEqual("200", status)

    def testSuggest(self):
        data = """{
            "value": "ha",
            "trigram": false,
            "filters": [],
            "keySetName": null
        }"""
        status, header, body = postRequest(self.suggestionServerPort, '/suggest', data=data)
        self.assertEqual("200", status)
        self.assertEqual("application/json", header['Content-Type'].split(';')[0])
        self.assertEqual([], body)

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
            status, header, body = postRequest(self.suggestionServerPort, '/suggest', data=data, parse=False)
            self.assertEqual("200", status)
            self.assertEqual([
                {"suggestion": "harry", "type": "uri:book", "creator": 'rowling', "score": 0.2615291476249695},
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
            status, header, body = getRequest(port=self.httpPort, path='/suggestion', arguments={'value': 'ha'}, parse=False)
            self.assertEqual(["ha", ["harry", "hallo"]], loads(body))

            status, header, body = getRequest(port=self.httpPort, path='/suggestion', arguments={'value': 'ha', "filter": "type=uri:book"}, parse=False)
            self.assertEqual(["ha", ["harry"]], loads(body))
        finally:
            postRequest(self.suggestionServerPort, '/delete?identifier=id1', data=None, parse=False)
            postRequest(self.suggestionServerPort, '/delete?identifier=id2', data=None, parse=False)
            postRequest(self.suggestionServerPort, '/commit', data=None, parse=False)

    def testIndexingState(self):
        status, header, body = getRequest(self.suggestionServerPort, '/indexingState')
        self.assertEqual("200", status)
        self.assertEqual({}, body)

        postRequest(self.suggestionServerPort, '/createSuggestionNGramIndex', data=None, parse=False)
        status, header, body = getRequest(self.suggestionServerPort, '/indexingState')
        self.assertEqual("200", status)
        self.assertTrue("started" in body, body)
        self.assertTrue("count" in body, body)

    def testRegisterKeySet(self):
        keySet = FixedBitSet(3)
        keySet.set(2)

        status, header, body = postRequest(self.suggestionServerPort, '/registerFilterKeySet?name=test', data=fixedBitSetAsBytes(keySet), parse=False)
        self.assertEqual("200", status)


def fixedBitSetAsBytes(bitSet):
    buf = BytesIO()
    buf.write(pack('>i', bitSet.length()))
    bits = bitSet.getBits()
    buf.write(pack('>i', len(bits)))
    for i in range(0, len(bits)):
        buf.write(pack('>q', bits[i]))
    return buf.getvalue()

## begin license ##
#
# "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
#
# Copyright (C) 2015 Koninklijke Bibliotheek (KB) http://www.kb.nl
# Copyright (C) 2015-2016 Seecr (Seek You Too B.V.) http://seecr.nl
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

from seecr.test import SeecrTestCase
from meresco.lucene.suggestionindexcomponent import SuggestionIndexComponent
from weightless.core import asString, consume, retval
from meresco.components.http.utils import CRLF
from simplejson import loads, dumps
from seecr.test.io import stdout_replaced

from org.apache.lucene.util import OpenBitSet


class SuggestionIndexComponentTest(SeecrTestCase):

    def setUp(self):
        SeecrTestCase.setUp(self)
        self.sic = SuggestionIndexComponent(host="localhost", port=12345)
        self.post = []
        self.response = ""
        def mockPost(data, path, **kwargs):
            self.post.append(dict(data=data, path=path))
            raise StopIteration(self.response)
            yield
        self.sic._connect._post = mockPost

        self.get = []
        def mockGet(path, **kwargs):
            self.get.append(path)
            raise StopIteration(self.response)
            yield
        self.sic._connect._get = mockGet

    def testAdd(self):
        consume(self.sic.addSuggestions(identifier="id:1", key=1, values=[dict(title="harry", type="uri:book", creator="rowling")]))
        self.assertEqual(1, len(self.post))
        self.assertEqual('/add?identifier=id%3A1', self.post[0]['path'])
        self.assertEqual({
                "key": 1,
                "values": ["harry"], "types": ["uri:book"], "creators": ["rowling"]
            }, loads(self.post[0]['data']))

    def testDelete(self):
        consume(self.sic.deleteSuggestions(identifier="id:1"))
        self.assertEqual(1, len(self.post))
        self.assertEqual('/delete?identifier=id%3A1', self.post[0]['path'])
        self.assertEqual(None, self.post[0]['data'])

    def testCreateNgramIndex(self):
        consume(self.sic.createSuggestionNGramIndex())
        self.assertEqual(1, len(self.post))
        self.assertEqual('/createSuggestionNGramIndex', self.post[0]['path'])
        self.assertEqual(None, self.post[0]['data'])

    def testSuggest(self):
        self.response = dumps([
            {"suggestion": "hallo", "type": "uri:book", "creator": 'by:me', "score": 1.0},
            {"suggestion": "harry", "type": None, "creator": None, "score": 1.0}
        ])
        suggestions = retval(self.sic.suggest("ha"))
        self.assertEqual(1, len(self.post))
        self.assertEqual('/suggest', self.post[0]['path'])
        self.assertEqual({"value": "ha", "trigram": False, "filters": [], "keySetName": None}, loads(self.post[0]['data']))

        self.assertEquals([u"hallo", u"harry"], [s.suggestion for s in suggestions])
        self.assertEquals([u"uri:book", None], [s.type for s in suggestions])
        self.assertEquals([u"by:me", None], [s.creator for s in suggestions])

    def testHandleRequest(self):
        self.response = dumps([
            {"suggestion": "hallo", "type": "uri:book", "creator": 'by:me', "score": 1.0},
            {"suggestion": "harry", "type": None, "creator": None, "score": 1.0}
        ])
        header, body = asString(self.sic.handleRequest(path='/suggestion', arguments=dict(value=["ha"], minScore=["0"]))).split(CRLF*2)
        self.assertEquals("""HTTP/1.0 200 OK\r
Content-Type: application/x-suggestions+json\r
Access-Control-Allow-Origin: *\r
Access-Control-Allow-Headers: X-Requested-With\r
Access-Control-Allow-Methods: GET, POST, OPTIONS\r
Access-Control-Max-Age: 86400""", header)
        self.assertEquals('["ha", ["hallo", "harry"]]', body)

    def testHandleRequestWithTypesAndCreators(self):
        self.response = dumps([
            {"suggestion": "hallo", "type": "uri:book", "creator": 'by:me', "score": 1.0},
            {"suggestion": "harry", "type": "uri:book", "creator": "rowling", "score": 1.0}
        ])
        header, body = asString(self.sic.handleRequest(path='/suggestion', arguments=dict(value=["ha"], minScore=["0"], concepts=["True"]))).split(CRLF*2)
        self.assertEquals("""HTTP/1.0 200 OK\r
Content-Type: application/x-suggestions+json\r
Access-Control-Allow-Origin: *\r
Access-Control-Allow-Headers: X-Requested-With\r
Access-Control-Allow-Methods: GET, POST, OPTIONS\r
Access-Control-Max-Age: 86400""", header)
        self.assertEquals('["ha", ["hallo", "harry"], [["hallo", "uri:book", "by:me"], ["harry", "uri:book", "rowling"]]]', body)

    def testHandleRequestWithDebug(self):
        self.response = dumps([
            {"suggestion": "hallo", "type": "uri:book", "creator": 'by:me', "score": 0.80111},
            {"suggestion": "harry", "type": "uri:book", "creator": "rowling", "score": 0.80111}
        ])
        header, body = asString(self.sic.handleRequest(path='/suggestion', arguments={"value": ["ha"], "x-debug": ["true"], "minScore": ["0"]})).split(CRLF*2)
        self.assertEquals("""HTTP/1.0 200 OK\r
Content-Type: application/x-suggestions+json\r
Access-Control-Allow-Origin: *\r
Access-Control-Allow-Headers: X-Requested-With\r
Access-Control-Allow-Methods: GET, POST, OPTIONS\r
Access-Control-Max-Age: 86400""", header)
        json = loads(body)
        self.assertEquals('ha', json['value'])
        self.assertTrue("time" in json, json)
        suggestions = [(s[0], dict((k,round(v, 3)) for k,v in s[3].items())) for s in json['suggestions']]
        self.assertEquals(sorted([
            ("hallo", {"distanceScore": 0.653, "score": 0.801, "sortScore": 0.839, "matchScore": 1.0}),
            ("harry", {"distanceScore": 0.653, "score": 0.801, "sortScore": 0.839, "matchScore": 1.0}),
            ]), sorted(suggestions))

    def testHandleRequestWithEmptyValue(self):
        self.response = dumps([])
        header, body = asString(self.sic.handleRequest(path='/suggestion', arguments={})).split(CRLF*2)
        self.assertEquals('[]', body)

    def testCommit(self):
        consume(self.sic.commit())
        self.assertEqual(1, len(self.post))
        self.assertEqual('/commit', self.post[0]['path'])
        self.assertEqual(None, self.post[0]['data'])

    def testTotalShingleRecords(self):
        self.response = "10"
        total = retval(self.sic.totalShingleRecords())
        self.assertEqual(10, total)
        self.assertEqual(0, len(self.post))
        self.assertEqual(1, len(self.get))
        self.assertEqual('/totalRecords', self.get[0])

    def testTotalSuggestions(self):
        self.response = "10"
        total = retval(self.sic.totalSuggestions())
        self.assertEqual(10, total)
        self.assertEqual(0, len(self.post))
        self.assertEqual(1, len(self.get))
        self.assertEqual('/totalSuggestions', self.get[0])

    def testSkipDuplicates(self):
        self.response = dumps([
            {"suggestion": "hallo", "type": "uri:book", "creator": 'by:me', "score": 1.0},
            {"suggestion": "harry", "type": "uri:book", "creator": 'rowling', "score": 1.0},
            {"suggestion": "harry", "type": "uri:track", "creator": None, "score": 1.0},
            {"suggestion": "harry", "type": "uri:e-book", "creator": None, "score": 1.0},
        ])
        header, body = asString(self.sic.handleRequest(path='/suggestion', arguments={"value": ["ha"], "concepts": "True", "minScore": ["0"]})).split(CRLF*2)
        self.assertEqual('["ha", ["hallo", "harry"], [["hallo", "uri:book", "by:me"], ["harry", "uri:book", "rowling"], ["harry", "uri:track", null], ["harry", "uri:e-book", null]]]', body)

    def testFilters(self):
        self.response = dumps([
            {"suggestion": "harry", "type": "uri:track", "creator": None, "score": 1.0},
        ])
        header, body = asString(self.sic.handleRequest(path='/suggestion', arguments={"value": ["ha"], "concepts": "True", "minScore": ["0"], "filter": ["type=uri:track"]})).split(CRLF*2)
        self.assertEqual('["ha", ["harry"], [["harry", "uri:track", null]]]', body)
        self.assertEqual(1, len(self.post))
        self.assertEqual({'data': '{"keySetName": null, "trigram": false, "value": "ha", "filters": ["type=uri:track"]}', 'path': '/suggest'}, self.post[0])

    def testFilterByKeySet(self):
        self.response = dumps([
            {"suggestion": "fietsbel", "type": "uri:book", "creator": None, "score": 1.0},
        ])

        # bits = OpenBitSet()
        # bits.set(2L)
        # consume(self.sic.registerFilterKeySet("apikey-abc", bits))

        header, body = asString(self.sic.handleRequest(path='/suggestion', arguments={"value": ["fi"], "apikey": ["apikey-abc"]})).split(CRLF*2)
        self.assertEqual('["fi", ["fietsbel"]]', body)
        self.assertEqual(1, len(self.post))
        self.assertEqual({'data': '{"keySetName": "apikey-abc", "trigram": false, "value": "fi", "filters": []}', 'path': '/suggest'}, self.post[0])




def createSuggestions():
    return [
            dict(title="harry", type="uri:book", creator="rowling"),
            dict(title="potter", type="uri:book", creator="rowling"),
            dict(title="hallo", type="uri:book", creator="by:me"),
            dict(title="fiets", type="uri:book", creator="by:me"),
            dict(title="fiets mobiel", type="uri:book")
        ]
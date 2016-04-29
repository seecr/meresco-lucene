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
from weightless.core import asString
from meresco.components.http.utils import CRLF
from simplejson import loads
from seecr.test.io import stdout_replaced

from org.apache.lucene.util import OpenBitSet
from time import time


class SuggestionIndexComponentTest(SeecrTestCase):
    def testSuggestionsAreEmptyIfNotCreated(self):
        sic = SuggestionIndexComponent(self.tempdir, commitCount=1)
        sic.addSuggestions(identifier="id:1", key=1, values=createSuggestions())
        self.assertEquals([], sic.suggest('ha'))

    def testSuggest(self):
        sic = SuggestionIndexComponent(self.tempdir, commitCount=1)
        sic.addSuggestions(identifier="id:1", key=1, values=createSuggestions())
        sic.createSuggestionNGramIndex(wait=True, verbose=False)

        suggestions = sic.suggest("ha")
        self.assertEquals([u"hallo", u"harry"], [s.suggestion for s in suggestions])

        suggestions = sic.suggest("fiet")
        self.assertEquals(["fiets", "fiets mobiel"], [s.suggestion for s in suggestions])

        self.assertEquals(5, sic.totalSuggestions())

    def testSuggestWithTypesAndCreators(self):
        sic = SuggestionIndexComponent(self.tempdir, commitCount=1)
        sic.addSuggestions(identifier="id:1", key=1, values=createSuggestions())
        sic.createSuggestionNGramIndex(wait=True, verbose=False)

        suggestions = sic.suggest("ha")
        self.assertEquals([u"hallo", u"harry"], [s.suggestion for s in suggestions])
        self.assertEquals([u"uri:book", u"uri:book"], [s.type for s in suggestions])
        self.assertEquals([u"by:me", u"rowling"], [s.creator for s in suggestions])

    def testTimestampNgramIndex(self):
        sic = SuggestionIndexComponent(self.tempdir, commitCount=1)
        timestamp = sic.ngramIndexTimestamp()
        now = time()
        # timestamp for files is not very precise
        self.assertTrue(now - 1 < timestamp < now + 1, (timestamp, now))

    def testHandleRequest(self):
        sic = SuggestionIndexComponent(self.tempdir, commitCount=1)
        sic.addSuggestions(identifier="id:1", key=1, values=createSuggestions())
        sic.createSuggestionNGramIndex(wait=True, verbose=False)
        header, body = asString(sic.handleRequest(path='/suggestion', arguments=dict(value=["ha"], minScore=["0"]))).split(CRLF*2)
        self.assertEquals("""HTTP/1.0 200 OK\r
Content-Type: application/x-suggestions+json\r
Access-Control-Allow-Origin: *\r
Access-Control-Allow-Headers: X-Requested-With\r
Access-Control-Allow-Methods: GET, POST, OPTIONS\r
Access-Control-Max-Age: 86400""", header)
        self.assertEquals('["ha", ["hallo", "harry"]]', body)

    def testHandleRequestWithTypesAndCreators(self):
        sic = SuggestionIndexComponent(self.tempdir, commitCount=1)
        sic.addSuggestions(identifier="id:1", key=1, values=createSuggestions())
        sic.createSuggestionNGramIndex(wait=True, verbose=False)
        header, body = asString(sic.handleRequest(path='/suggestion', arguments=dict(value=["ha"], minScore=["0"], concepts=["True"]))).split(CRLF*2)
        self.assertEquals("""HTTP/1.0 200 OK\r
Content-Type: application/x-suggestions+json\r
Access-Control-Allow-Origin: *\r
Access-Control-Allow-Headers: X-Requested-With\r
Access-Control-Allow-Methods: GET, POST, OPTIONS\r
Access-Control-Max-Age: 86400""", header)
        self.assertEquals('["ha", ["hallo", "harry"], [["hallo", "uri:book", "by:me"], ["harry", "uri:book", "rowling"]]]', body)

    def testHandleRequestWithDebug(self):
        sic = SuggestionIndexComponent(self.tempdir, commitCount=1)
        sic.addSuggestions(identifier="id:1", key=1, values=createSuggestions())
        sic.createSuggestionNGramIndex(wait=True, verbose=False)
        header, body = asString(sic.handleRequest(path='/suggestion', arguments={"value": ["ha"], "x-debug": ["true"], "minScore": ["0"]})).split(CRLF*2)
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
        sic = SuggestionIndexComponent(self.tempdir, commitCount=1)
        header, body = asString(sic.handleRequest(path='/suggestion', arguments={})).split(CRLF*2)
        self.assertEquals('[]', body)

    @stdout_replaced
    def testPersistentShingles(self):
        sic = SuggestionIndexComponent(self.tempdir, commitCount=1)
        sic.addSuggestions(identifier="id:1", key=1, values=createSuggestions())
        sic.createSuggestionNGramIndex(wait=True, verbose=False)
        sic.handleShutdown()

        sic = SuggestionIndexComponent(self.tempdir, commitCount=1)
        suggestions = sic.suggest("ha")
        self.assertEquals([u"hallo", u"harry"], [s.suggestion for s in suggestions])

    def testAddDelete(self):
        sic = SuggestionIndexComponent(self.tempdir, commitCount=1)
        sic.addSuggestions(identifier="id:1", key=1, values=createSuggestions())
        sic.addSuggestions(identifier="id:2", key=1, values=createSuggestions())
        self.assertEquals(2, sic.totalShingleRecords())
        sic.deleteSuggestions("id:1")
        self.assertEquals(1, sic.totalShingleRecords())

    def testSkipDuplicates(self):
        sic = SuggestionIndexComponent(self.tempdir, commitCount=1)
        suggestions = createSuggestions()
        suggestions.append(dict(title="harry", type="uri:e-book"))
        suggestions.append(dict(title="harry", type="uri:track"))
        sic.addSuggestions(identifier="id:1", key=1, values=suggestions)
        sic.createSuggestionNGramIndex(wait=True, verbose=False)
        header, body = asString(sic.handleRequest(path='/suggestion', arguments={"value": ["ha"], "concepts": "True", "minScore": ["0"]})).split(CRLF*2)
        self.assertEqual('["ha", ["hallo", "harry"], [["hallo", "uri:book", "by:me"], ["harry", "uri:book", "rowling"], ["harry", "uri:e-book", null], ["harry", "uri:track", null]]]', body)

    def testFilters(self):
        sic = SuggestionIndexComponent(self.tempdir, commitCount=1)
        suggestions = createSuggestions()
        suggestions.append(dict(title="harry", type="uri:e-book"))
        suggestions.append(dict(title="harry", type="uri:track"))
        sic.addSuggestions(identifier="id:1", key=1, values=suggestions)
        sic.createSuggestionNGramIndex(wait=True, verbose=False)
        header, body = asString(sic.handleRequest(path='/suggestion', arguments={"value": ["ha"], "concepts": "True", "minScore": ["0"], "filter": ["type=uri:track"]})).split(CRLF*2)
        self.assertEqual('["ha", ["harry"], [["harry", "uri:track", null]]]', body)

    def testFilterByKeySet(self):
        sic = SuggestionIndexComponent(self.tempdir, commitCount=1)
        suggestions = createSuggestions()
        sic.addSuggestions(identifier="id:1", key=1, values=suggestions)
        suggestions = [
            dict(title="fietsbel", type="uri:book", creator="Anonymous")
        ]
        sic.addSuggestions(identifier="id:2", key=2, values=suggestions)
        sic.createSuggestionNGramIndex(wait=True, verbose=False)
        bits = OpenBitSet()
        bits.set(2L)
        sic.registerFilterKeySet("apikey-abc", bits)

        header, body = asString(sic.handleRequest(path='/suggestion', arguments={"value": ["fi"], "apikey": ["apikey-abc"]})).split(CRLF*2)
        self.assertEqual('["fi", ["fietsbel"]]', body)
        header, body = asString(sic.handleRequest(path='/suggestion', arguments={"value": ["fi"], "apikey": ["apikey-never-registered"]})).split(CRLF*2)
        self.assertEqual('["fi", ["fiets", "fietsbel", "fiets mobiel"]]', body)


def createSuggestions():
    return [
            dict(title="harry", type="uri:book", creator="rowling"),
            dict(title="potter", type="uri:book", creator="rowling"),
            dict(title="hallo", type="uri:book", creator="by:me"),
            dict(title="fiets", type="uri:book", creator="by:me"),
            dict(title="fiets mobiel", type="uri:book")
        ]
## begin license ##
#
# "NBC+" also known as "ZP (ZoekPlatform)" is
#  a project of the Koninklijke Bibliotheek
#  and provides a search service for all public
#  libraries in the Netherlands.
#
# Copyright (C) 2015 Koninklijke Bibliotheek (KB) http://www.kb.nl
# Copyright (C) 2015 Seecr (Seek You Too B.V.) http://seecr.nl
#
# This file is part of "NBC+ (Zoekplatform BNL)"
#
# "NBC+ (Zoekplatform BNL)" is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# "NBC+ (Zoekplatform BNL)" is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with "NBC+ (Zoekplatform BNL)"; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
#
## end license ##

from seecr.test import SeecrTestCase
from meresco.lucene.suggestionindexcomponent import SuggestionIndexComponent
from weightless.core import asString
from meresco.components.http.utils import CRLF
from simplejson import loads
from seecr.test.io import stdout_replaced

class SuggestionIndexComponentTest(SeecrTestCase):

    def testSuggestionsAreEmptyIfNotCreated(self):
        sic = SuggestionIndexComponent(self.tempdir, commitCount=1)
        sic.addSuggestions("id:1", [("harry", "uri:book"), ("potter", "uri:book"), ("hallo", "uri:book"), ("fiets", "uri:book"), ("fiets mobiel", "uri:book")])
        self.assertEquals([], sic.suggest('ha'))

    def testSuggest(self):
        sic = SuggestionIndexComponent(self.tempdir, commitCount=1)
        sic.addSuggestions("id:1", [("harry", "uri:book"), ("potter", "uri:book"), ("hallo", "uri:book"), ("fiets", "uri:book"), ("fiets mobiel", "uri:book")])
        sic.createSuggestionNGramIndex(wait=True, verbose=False)

        suggestions = sic.suggest("ha")
        self.assertEquals([u"hallo", u"harry"], [s.suggestion for s in suggestions])

        suggestions = sic.suggest("fiet")
        self.assertEquals(["fiets", "fiets mobiel"], [s.suggestion for s in suggestions])

        self.assertEquals(5, sic.totalSuggestions())

    def testSuggestWithTypes(self):
        sic = SuggestionIndexComponent(self.tempdir, commitCount=1)
        sic.addSuggestions("id:1", [("harry potter", "uri:book"), ("hallo", "uri:book")])
        sic.createSuggestionNGramIndex(wait=True, verbose=False)

        suggestions = sic.suggest("ha")
        self.assertEquals([u"hallo", u"harry potter"], [s.suggestion for s in suggestions])
        self.assertEquals([u"uri:book", u"uri:book"], [s.type for s in suggestions])

    def testSuggestWithCreators(self):
        sic = SuggestionIndexComponent(self.tempdir, commitCount=1)
        sic.addSuggestions("id:1", [("harry potter", "uri:book", "rowling"), ("hallo", "uri:book", "by:me")])
        sic.createSuggestionNGramIndex(wait=True, verbose=False)

        suggestions = sic.suggest("ha")
        self.assertEquals([u"hallo", u"harry potter"], [s.suggestion for s in suggestions])
        self.assertEquals([u"uri:book", u"uri:book"], [s.type for s in suggestions])
        self.assertEquals([u"uri:book", u"uri:book"], [s.creator for s in suggestions])

    def testHandleRequest(self):
        sic = SuggestionIndexComponent(self.tempdir, commitCount=1)
        sic.addSuggestions("id:1", [("harry", "uri:book"), ("potter", "uri:book"), ("hallo", "uri:book"), ("fiets", "uri:book"), ("fiets mobiel", "uri:book")])
        sic.createSuggestionNGramIndex(wait=True, verbose=False)
        header, body = asString(sic.handleRequest(path='/suggestion', arguments=dict(value=["ha"], minScore=["0"]))).split(CRLF*2)
        self.assertEquals("""HTTP/1.0 200 OK\r
Content-Type: application/x-suggestions+json\r
Access-Control-Allow-Origin: *\r
Access-Control-Allow-Headers: X-Requested-With""", header)
        self.assertEquals('["ha", ["hallo", "harry"]]', body)

    def testHandleRequestWithTypes(self):
        sic = SuggestionIndexComponent(self.tempdir, commitCount=1)
        sic.addSuggestions("id:1", [("harry", 'uri:harry'), ("potter", "uri:book"), ("hallo", "uri:book"), ("fiets", 'uri:fiets'), ("fiets mobiel", "uri:book")])
        sic.createSuggestionNGramIndex(wait=True, verbose=False)
        header, body = asString(sic.handleRequest(path='/suggestion', arguments=dict(value=["ha"], minScore=["0"], concepts=["True"]))).split(CRLF*2)
        self.assertEquals("""HTTP/1.0 200 OK\r
Content-Type: application/x-suggestions+json\r
Access-Control-Allow-Origin: *\r
Access-Control-Allow-Headers: X-Requested-With""", header)
        self.assertEquals('["ha", ["hallo", "harry"], [["hallo", "uri:book"], ["harry", "uri:harry"]]]', body)

    def testHandleRequestWithDebug(self):
        sic = SuggestionIndexComponent(self.tempdir, commitCount=1)
        sic.addSuggestions("id:1", [("harry", "uri:book"), ("potter", "uri:book"), ("hallo", "uri:book"), ("fiets", "uri:book"), ("fiets mobiel", "uri:book")])
        sic.createSuggestionNGramIndex(wait=True, verbose=False)
        header, body = asString(sic.handleRequest(path='/suggestion', arguments={"value": ["ha"], "x-debug": ["true"], "minScore": ["0"]})).split(CRLF*2)
        self.assertEquals("""HTTP/1.0 200 OK\r
Content-Type: application/x-suggestions+json\r
Access-Control-Allow-Origin: *\r
Access-Control-Allow-Headers: X-Requested-With""", header)
        json = loads(body)
        self.assertEquals('ha', json['value'])
        self.assertTrue("time" in json, json)
        suggestions = [(s[0], dict((k,round(v, 3)) for k,v in s[2].items())) for s in json['suggestions']]
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
        sic.addSuggestions("id:1", [("harry", "uri:book"), ("potter", "uri:book"), ("hallo", "uri:book"), ("fiets", "uri:book"), ("fiets mobiel", "uri:book")])
        sic.createSuggestionNGramIndex(wait=True, verbose=False)
        sic.handleShutdown()

        sic = SuggestionIndexComponent(self.tempdir, commitCount=1)
        suggestions = sic.suggest("ha")
        self.assertEquals([u"hallo", u"harry"], [s.suggestion for s in suggestions])

    def testAddDelete(self):
        sic = SuggestionIndexComponent(self.tempdir, commitCount=1)
        sic.addSuggestions("id:1", [("harry", "uri:book"), ("fiets", "uri:book")])
        sic.addSuggestions("id:2", [("harry potter", "uri:book")])
        self.assertEquals(2, sic.totalShingleRecords())
        sic.deleteSuggestions("id:1")
        self.assertEquals(1, sic.totalShingleRecords())

    def testSkipDuplicates(self):
        sic = SuggestionIndexComponent(self.tempdir, commitCount=1)
        sic.addSuggestions("id:1", [("harry", "uri:book"), ("harry", "uri:e-book"), ("harry", "uri:track"), ("harry potter", "uri:person")])
        sic.createSuggestionNGramIndex(wait=True, verbose=False)
        header, body = asString(sic.handleRequest(path='/suggestion', arguments={"value": ["ha"], "concepts": "True", "minScore": ["0"]})).split(CRLF*2)
        self.assertEqual('["ha", ["harry", "harry potter"], [["harry", "uri:book"], ["harry", "uri:e-book"], ["harry", "uri:track"], ["harry potter", "uri:person"]]]', body)

    def testFilters(self):
        sic = SuggestionIndexComponent(self.tempdir, commitCount=1)
        sic.addSuggestions("id:1", [("harry", "uri:book"), ("harry", "uri:e-book"), ("harry", "uri:track"), ("harry potter", "uri:person")])
        sic.createSuggestionNGramIndex(wait=True, verbose=False)
        header, body = asString(sic.handleRequest(path='/suggestion', arguments={"value": ["ha"], "concepts": "True", "minScore": ["0"], "filter": ["type=uri:person"]})).split(CRLF*2)
        self.assertEqual('["ha", ["harry potter"], [["harry potter", "uri:person"]]]', body)

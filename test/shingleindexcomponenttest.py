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
from meresco.lucene.shingleindexcomponent import ShingleIndexComponent
from weightless.core import asString
from meresco.components.http.utils import CRLF
from simplejson import loads
from seecr.test.io import stdout_replaced

class ShingleIndexComponentTest(SeecrTestCase):

    def testSuggestionsAreEmptyIfNotCreated(self):
        sic = ShingleIndexComponent(self.tempdir, commitCount=1)
        sic.addSuggestions("id:1", ["harry", "potter", "hallo", "fiets", "fiets mobiel"])
        self.assertEquals([], sic.suggest('ha'))

    def testSuggest(self):
        sic = ShingleIndexComponent(self.tempdir, commitCount=1)
        sic.addSuggestions("id:1", ["harry", "potter", "hallo", "fiets", "fiets mobiel"])
        sic.createSuggestionIndex(wait=True, verbose=False)

        suggestions = sic.suggest("ha")
        self.assertEquals([u"hallo", u"harry"], [s.suggestion for s in suggestions])

        suggestions = sic.suggest("fiet")
        self.assertEquals(["fiets", "fiets mobiel"], [s.suggestion for s in suggestions])

        self.assertEquals(6, sic.totalSuggestions())

    def testHandleRequest(self):
        sic = ShingleIndexComponent(self.tempdir, commitCount=1)
        sic.addSuggestions("id:1", ["harry", "potter", "hallo", "fiets", "fiets mobiel"])
        sic.createSuggestionIndex(wait=True, verbose=False)
        header, body = asString(sic.handleRequest(path='/suggestion', arguments=dict(value=["ha"], minScore=["0"]))).split(CRLF*2)
        self.assertEquals("""HTTP/1.0 200 Ok\r
Content-Type: application/json""", header)
        self.assertEquals('["ha", ["hallo", "harry"]]', body)

    def testHandleRequestWithDebug(self):
        sic = ShingleIndexComponent(self.tempdir, commitCount=1)
        sic.addSuggestions("id:1", ["harry", "potter", "hallo", "fiets", "fiets mobiel"])
        sic.createSuggestionIndex(wait=True, verbose=False)
        header, body = asString(sic.handleRequest(path='/suggestion', arguments=dict(value=["ha"], debug=["true"], minScore=["0"]))).split(CRLF*2)
        self.assertEquals("""HTTP/1.0 200 Ok\r
Content-Type: application/json""", header)
        json = loads(body)
        suggestions = [(s[0], dict((k,round(v, 3)) for k,v in s[1].items())) for s in json[1]]
        self.assertEquals([
            ("hallo", {"distanceScore": 0.653, "score": 0.003, "sortScore": 0.0}),
            ("harry", {"distanceScore": 0.653, "score": 0.003, "sortScore": 0.0}),
            ], suggestions)

    def testHandleRequestWithEmptyValue(self):
        sic = ShingleIndexComponent(self.tempdir, commitCount=1)
        header, body = asString(sic.handleRequest(path='/suggestion', arguments={})).split(CRLF*2)
        self.assertEquals('[]', body)

    @stdout_replaced
    def testPersistentShingles(self):
        sic = ShingleIndexComponent(self.tempdir, commitCount=1)
        sic.addSuggestions("id:1", ["harry", "potter", "hallo", "fiets", "fiets mobiel"])
        sic.createSuggestionIndex(wait=True, verbose=False)
        sic.handleShutdown()

        sic = ShingleIndexComponent(self.tempdir, commitCount=1)
        suggestions = sic.suggest("ha")
        self.assertEquals([u"hallo", u"harry"], [s.suggestion for s in suggestions])

    def testAddDelete(self):
        sic = ShingleIndexComponent(self.tempdir, commitCount=1)
        sic.addSuggestions("id:1", ["harry", "fiets"])
        sic.addSuggestions("id:2", ["harry potter"])
        self.assertEquals(2, sic.totalShingleRecords())
        sic.deleteSuggestions("id:1")
        self.assertEquals(1, sic.totalShingleRecords())


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

from org.meresco.lucene.suggestion import ShingleIndex
from os.path import isdir, join
from os import makedirs
from meresco.core import Observable
from meresco.components.http.utils import CRLF
from meresco.components.json import JsonList
from Levenshtein import distance
from math import log

class ShingleIndexComponent(Observable):

    def __init__(self, stateDir, minShingles=2, maxShingles=6, commitCount=10000, **kwargs):
        super(ShingleIndexComponent, self).__init__(**kwargs)
        self._shingleIndexDir = join(stateDir, 'shingles')
        self._suggestionIndexDir = join(stateDir, 'suggestions')
        isdir(self._shingleIndexDir) or makedirs(self._shingleIndexDir)
        isdir(self._suggestionIndexDir) or makedirs(self._suggestionIndexDir)
        self._index = ShingleIndex(self._shingleIndexDir, self._suggestionIndexDir, minShingles, maxShingles, commitCount)
        self._reader = self._index.getSuggestionsReader()

    def addSuggestions(self, identifier, values):
        self._index.add(identifier, values)

    def deleteSuggestions(self, identifier):
        self._index.delete(identifier)

    def createSuggestionIndex(self, wait=False, verbose=True):
        self._index.createSuggestionIndex(wait, verbose)

    def suggest(self, value, trigram=False):
        if not self._reader:
            return []
        return list(self._reader.suggest(value, trigram))

    def indexingState(self):
        indexingState = self._index.indexingState()
        if indexingState is not None:
            return dict(started=int(indexingState.started), count=int(indexingState.count))
        return None

    def totalShingleRecords(self):
        return int(self._index.numDocs())

    def totalSuggestions(self):
        return int(self._reader.numDocs())

    def handleRequest(self, arguments, path, **kwargs):
        value = arguments.get("value", [None])[0]
        debug = arguments.get("debug", ["False"])[0] != 'False'
        trigram = arguments.get("trigram", ["False"])[0] != 'False'
        minScore = float(arguments.get("minScore", ["0.03"])[0])
        yield "HTTP/1.0 200 Ok" + CRLF
        yield "Content-Type: application/json" + CRLF
        yield CRLF
        result = []
        if value:
            suggestions = []
            for s in self.suggest(value, trigram=trigram):
                suggestion = str(s.suggestion)
                distanceScore = max(0, -log(distance(value, suggestion) + 1) / 4 + 1)
                score = float(s.score)
                sortScore = distanceScore*score**2
                scores = dict(distanceScore=distanceScore, score=score, sortScore=sortScore)
                if sortScore > minScore:
                    suggestions.append((suggestion, scores))
            suggestions = sorted(suggestions, reverse=True, key=lambda (suggestion, scores): scores['sortScore'])
            if not debug:
                suggestions = [s[0] for s in suggestions[:10]]
            result = [value, suggestions]
        yield JsonList(result).dumps()

    def handleShutdown(self):
        print 'handle shutdown: saving ShingleIndexComponent'
        from sys import stdout; stdout.flush()
        self._index.close()
        self._reader.close()

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

from org.meresco.lucene.suggestion import SuggestionIndex
from os.path import isdir, join
from os import makedirs
from meresco.core import Observable
from meresco.components.http.utils import CRLF, ContentTypeHeader, Ok
from meresco.components.json import JsonList, JsonDict
from Levenshtein import distance
from math import log
from time import time

class SuggestionIndexComponent(Observable):

    def __init__(self, stateDir, minShingles=2, maxShingles=6, commitCount=10000, **kwargs):
        super(SuggestionIndexComponent, self).__init__(**kwargs)
        self._suggestionIndexDir = join(stateDir, 'suggestions')
        self._ngramIndexDir = join(stateDir, 'ngram')
        isdir(self._suggestionIndexDir) or makedirs(self._suggestionIndexDir)
        isdir(self._ngramIndexDir) or makedirs(self._ngramIndexDir)
        self._index = SuggestionIndex(self._suggestionIndexDir, self._ngramIndexDir, minShingles, maxShingles, commitCount)
        self._reader = self._index.getSuggestionsReader()

    def addSuggestions(self, identifier, values):
        titles = [v[0] for v in values]
        types = [v[1] for v in values]
        self._index.add(identifier, titles, types)

    def deleteSuggestions(self, identifier):
        self._index.delete(identifier)

    def createSuggestionNGramIndex(self, wait=False, verbose=True):
        self._index.createSuggestionNGramIndex(wait, verbose)

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
        debug = arguments.get("x-debug", ["False"])[0] != 'False'
        trigram = arguments.get("trigram", ["False"])[0] != 'False'
        showConcepts = arguments.get("concepts", ["False"])[0] != 'False'
        minScore = float(arguments.get("minScore", ["0"])[0])
        yield Ok
        yield ContentTypeHeader + "application/x-suggestions+json" + CRLF
        yield "Access-Control-Allow-Origin: *" + CRLF
        yield "Access-Control-Allow-Headers: X-Requested-With" + CRLF
        yield CRLF
        result = []
        if value:
            suggestions = []
            t0 = time()
            suggest = self.suggest(value, trigram=trigram)
            tTotal = time() - t0
            for s in suggest:
                suggestion = str(s.suggestion)
                recordType = str(s.type) if s.type else None
                distanceScore = max(0, -log(distance(value.lower(), suggestion.lower()) + 1) / 4 + 1)
                matchScore = match(value.lower(), suggestion.lower())
                score = float(s.score)
                sortScore = distanceScore * score**2 * (matchScore + 1)
                scores = dict(distanceScore=distanceScore, score=score, sortScore=sortScore, matchScore=matchScore)
                if sortScore > minScore:
                    suggestions.append((suggestion, recordType, scores))
            suggestions = sorted(suggestions, reverse=True, key=lambda (suggestion, recordType, scores): scores['sortScore'])
            if debug:
                concepts = [(s, t) for s, t, _ in suggestions if t]
                yield JsonDict(dict(value=value, suggestions=suggestions, concepts=concepts, time=tTotal)).dumps()
                return
            concepts = [(s, t) for s, t, _ in suggestions if t][:10]
            dedupSuggestions = []
            for s in suggestions:
                if s[0] not in dedupSuggestions:
                    dedupSuggestions.append(s[0])
            suggestions = dedupSuggestions[:10]
            result = [value, suggestions]
            if showConcepts:
                result.append(concepts)
        yield JsonList(result).dumps()

    def handleShutdown(self):
        print 'handle shutdown: saving ShingleIndexComponent'
        from sys import stdout; stdout.flush()
        self._index.close()
        self._reader.close()

def match(value, suggestion):
    matches = 0
    for v in value.split():
        if v in suggestion:
            matches += 1
    return matches
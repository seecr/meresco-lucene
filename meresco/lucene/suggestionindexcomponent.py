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

from os import makedirs
from os.path import isdir, join
from math import log
from time import time

from Levenshtein import distance

from meresco.core import Observable
from meresco.components.http.utils import CRLF, ContentTypeHeader, Ok, serverErrorPlainText
from meresco.components.json import JsonList, JsonDict

from org.meresco.lucene.suggestion import SuggestionIndex


class SuggestionIndexComponent(Observable):
    def __init__(self, stateDir, minShingles=2, maxShingles=6, commitCount=10000, **kwargs):
        super(SuggestionIndexComponent, self).__init__(**kwargs)
        self._suggestionIndexDir = join(stateDir, 'suggestions')
        self._ngramIndexDir = join(stateDir, 'ngram')
        isdir(self._suggestionIndexDir) or makedirs(self._suggestionIndexDir)
        isdir(self._ngramIndexDir) or makedirs(self._ngramIndexDir)
        self._index = SuggestionIndex(self._suggestionIndexDir, self._ngramIndexDir, minShingles, maxShingles, commitCount)
        self._reader = self._index.getSuggestionsReader()

    def addSuggestions(self, identifier, key, values):
        titles = [v.get('title') for v in values]
        types = [v.get('type') for v in values]
        creators = [v.get('creator') for v in values]
        self._index.add(identifier, key, titles, types, creators)

    def deleteSuggestions(self, identifier):
        self._index.delete(identifier)

    def registerFilterKeySet(self, name, keySet):
        self._index.registerFilterKeySet(name, keySet)

    def createSuggestionNGramIndex(self, wait=False, verbose=True):
        self._index.createSuggestionNGramIndex(wait, verbose)

    def suggest(self, value, trigram=False, filters=None, keySetName=None):
        if not self._reader:
            return []
        return list(self._reader.suggest(value, trigram, filters, keySetName))

    def indexingState(self):
        indexingState = self._index.indexingState()
        if indexingState is not None:
            return dict(started=int(indexingState.started), count=int(indexingState.count))
        return None

    def totalShingleRecords(self):
        return int(self._index.numDocs())

    def totalSuggestions(self):
        return int(self._reader.numDocs())

    def ngramIndexTimestamp(self):
        return self._index.ngramIndexTimestamp() / 1000.0

    def handleRequest(self, arguments, path, **kwargs):
        value = arguments.get("value", [None])[0]
        debug = arguments.get("x-debug", ["False"])[0] != 'False'
        trigram = arguments.get("trigram", ["False"])[0] != 'False'
        showConcepts = arguments.get("concepts", ["False"])[0] != 'False'
        filters = arguments.get("filter", None)
        minScore = float(arguments.get("minScore", ["0"])[0])
        apikey = arguments.get("apikey", [None])[0]
        apikeyFilter = arguments.get("x-apikey-filter", [''])[0]
        if apikeyFilter:
            apikey += "-" + apikeyFilter
        suggest = None
        if value:
            t0 = time()
            try:
                suggest = self.suggest(value, trigram=trigram, filters=filters, keySetName=apikey)
            except Exception, e:
                yield serverErrorPlainText
                yield str(e)
                return
            tTotal = time() - t0
        yield Ok
        yield ContentTypeHeader + "application/x-suggestions+json" + CRLF
        yield "Access-Control-Allow-Origin: *" + CRLF
        yield "Access-Control-Allow-Headers: X-Requested-With" + CRLF
        yield 'Access-Control-Allow-Methods: GET, POST, OPTIONS' + CRLF
        yield 'Access-Control-Max-Age: 86400' + CRLF
        yield CRLF
        result = []
        if value:
            suggestions = []
            for s in suggest:
                suggestion = str(s.suggestion)
                recordType = str(s.type) if s.type else None
                creator = str(s.creator) if s.creator else None
                distanceScore = max(0, -log(distance(value.lower(), suggestion.lower()) + 1) / 4 + 1)
                matchScore = match(value.lower(), suggestion.lower())
                score = float(s.score)
                sortScore = distanceScore * score**2 * (matchScore * 2)
                scores = dict(distanceScore=distanceScore, score=score, sortScore=sortScore, matchScore=matchScore)
                if sortScore > minScore:
                    suggestions.append((suggestion, recordType, creator, scores))
            suggestions = sorted(suggestions, reverse=True, key=lambda (suggestion, recordType, creator, scores): scores['sortScore'])
            if debug:
                concepts = [(s, t, c) for s, t, c, _ in suggestions if t]
                yield JsonDict(dict(value=value, suggestions=suggestions, concepts=concepts, time=tTotal)).dumps()
                return
            concepts = [(s, t, c) for s, t, c, _ in suggestions if t][:10]
            dedupSuggestions = []
            for s in suggestions:
                if s[0] not in dedupSuggestions:
                    dedupSuggestions.append(s[0])
            suggestions = dedupSuggestions[:10]
            result = [value, suggestions]
            if showConcepts:
                result.append(concepts)
        yield JsonList(result).dumps()

    def commit(self):
        self._index.commit()

    def handleShutdown(self):
        print 'handle shutdown: saving SuggestionIndexComponent'
        from sys import stdout; stdout.flush()
        self._index.close()
        self._reader.close()


def match(value, suggestion):
    matches = 0
    for v in value.split():
        if v in suggestion:
            matches += 1
    return matches

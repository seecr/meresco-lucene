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

from math import log
from time import time

from Levenshtein import distance

from meresco.core import Observable
from meresco.components.http.utils import CRLF, ContentTypeHeader, Ok, serverErrorPlainText
from meresco.components.json import JsonList, JsonDict

from _connect import _Connect
from urllib import urlencode
from weightless.io.utils import asProcess


class SuggestionIndexComponent(Observable):

    def __init__(self, host, port, **kwargs):
        super(SuggestionIndexComponent, self).__init__(**kwargs)
        self._connect = _Connect(host, port, observable=self)

    def addSuggestions(self, identifier, key, values):
        titles = [v.get('title') for v in values]
        types = [v.get('type') for v in values]
        creators = [v.get('creator') for v in values]
        yield self._connect.send("/add?{}".format(urlencode(dict(identifier=identifier))), JsonDict(key=key, values=titles, types=types, creators=creators))

    def deleteSuggestions(self, identifier):
        yield self._connect.send("/delete?{}".format(urlencode(dict(identifier=identifier))))

    def registerFilterKeySet(self, name, keySet):
        yield self._connect.send("/registerFilterKeySet?{}".format(urlencode(dict(name=name))), data=keySet)

    def createSuggestionNGramIndex(self):
        yield self._connect.send("/createSuggestionNGramIndex")

    def suggest(self, value, trigram=False, filters=None, keySetName=None):
        suggestions = yield self._connect.send("/suggest", JsonDict(value=value, trigram=trigram, filters=filters or [], keySetName=keySetName))
        raise StopIteration([Suggestion(s) for s in suggestions])

    def indexingState(self):
        indexingState = yield self._connect.read("/indexingState")
        raise StopIteration(indexingState if indexingState else None)

    def totalShingleRecords(self):
        total = yield self._connect.read("/totalRecords", parse=False)
        raise StopIteration(int(total))

    def totalSuggestions(self):
        total = yield self._connect.read("/totalSuggestions", parse=False)
        raise StopIteration(int(total))

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
                suggest = yield self.suggest(value, trigram=trigram, filters=filters, keySetName=apikey)
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
        asProcess(self._connect.send("/commit"))

class Suggestion(dict):
    def __getattr__(self, key):
        return self[key]

def match(value, suggestion):
    matches = 0
    for v in value.split():
        if v in suggestion:
            matches += 1
    return matches

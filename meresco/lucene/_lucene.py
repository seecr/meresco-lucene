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

from urllib import urlencode

from weightless.core import consume
from meresco.core import Observable
from meresco.components.json import JsonList, JsonDict
from meresco.lucene import LuceneResponse
from meresco.lucene.hit import Hit

from .utils import simplifiedDict
from _client import Client


class Lucene(Observable):
    def __init__(self, host, port, settings, name, **kwargs):
        Observable.__init__(self, name=name, **kwargs)
        self._client = Client(host, port, pathPrefix = "/" + name)
        self.settings = settings
        self._fieldRegistry = settings.fieldRegistry
        self._name = name

    def observer_init(self):
        consume(self._client.send(jsonDict=self.settings.asPostDict(), path="/settings/", synchronous=True))

    def setSettings(self, clusteringEps=None, clusteringMinPoints=None, clusterMoreRecords=None, similarity=None, numberOfConcurrentTasks=None, clusterFields=None):
        settingsDict = JsonDict()
        if numberOfConcurrentTasks:
            settingsDict["numberOfConcurrentTasks"] = numberOfConcurrentTasks
        if clusterMoreRecords:
            settingsDict["clusterMoreRecords"] = clusterMoreRecords
        if clusteringEps:
            settingsDict["clusteringEps"] = clusteringEps
        if clusteringMinPoints:
            settingsDict["clusteringMinPoints"] = clusteringMinPoints
        if clusterFields:
            settingsDict["clusterFields"] = clusterFields
        if similarity:
            settingsDict["similarity"] = dict(type="BM25Similarity", k1=similarity['k1'], b=similarity['b'])
        if settingsDict:
            yield self._client.send(jsonDict=settingsDict, path="/settings/")

    def getSettings(self):
        raise StopIteration((yield self._client.read(path='/settings/')))

    def addDocument(self, fields, identifier=None):
        args = urlencode(dict(identifier=identifier)) if identifier else ''
        yield self._client.send(jsonDict=JsonList(fields), path='/update/?{}'.format(args))

    def delete(self, identifier):
        yield self._client.send(path='/delete/?{}'.format(urlencode(dict(identifier=identifier))))

    def updateSortKey(self, sortKey):
        missingValue = self._fieldRegistry.defaultMissingValueForSort(sortKey["sortBy"], sortKey["sortDescending"])
        if missingValue:
            sortKey["missingValue"] = missingValue
        sortKey["type"] = self._fieldRegistry.sortFieldType(sortKey["sortBy"])

    def executeQuery(self, luceneQuery, start=None, stop=None, facets=None, sortKeys=None, suggestionRequest=None, dedupField=None, dedupSortField=None, groupingField=None, clustering=False, **kwargs):
        stop = 10 if stop is None else stop
        start = 0 if start is None else start

        for sortKey in sortKeys or []:
            self.updateSortKey(sortKey)
        jsonDict = JsonDict(
            query=luceneQuery,
            start=start,
            stop=stop,
            facets=facets or [],
            sortKeys=sortKeys or [],
            dedupField=dedupField,
            dedupSortField=dedupSortField,
            groupingField=groupingField,
            clustering=clustering,
        )
        if suggestionRequest:
            jsonDict["suggestionRequest"] = suggestionRequest
        responseDict = (yield self._client.send(jsonDict=jsonDict, path='/query/'))
        response = luceneResponseFromDict(responseDict)
        response.info = {
            'type': 'Query',
            'query': simplifiedDict(dict(
                    luceneQuery=luceneQuery,
                    start=start,
                    stop=stop,
                    facets=facets,
                    suggestionRequest=suggestionRequest,
                    **kwargs
                ))
            }
        raise StopIteration(response)
        yield

    def prefixSearch(self, fieldname, prefix, showCount=False, limit=10, **kwargs):
        jsonDict = JsonDict(
            fieldname=fieldname,
            prefix=prefix,
            limit=limit,
        )
        args = urlencode(dict(fieldname=fieldname, prefix=prefix, limit=limit))
        responseDict = (yield self._client.send(jsonDict=jsonDict, path='/prefixSearch/?{}'.format(args)))
        hits = [((term, count) if showCount else term) for term, count in sorted(responseDict, key=lambda t: t[1], reverse=True)]
        response = LuceneResponse(total=len(hits), hits=hits)
        raise StopIteration(response)
        yield

    def fieldnames(self, **kwargs):
        fieldnames = (yield self._client.send(path='/fieldnames/'))
        raise StopIteration(LuceneResponse(total=len(fieldnames), hits=fieldnames))
        yield

    def drilldownFieldnames(self, path=None, limit=50, **kwargs):
        args = dict(limit=limit)
        if path:
            args["dim"] = path[0]
            args["path"] = path[1:]
        args = urlencode(args, doseq=True)
        fieldnames = (yield self._client.send(path='/drilldownFieldnames/?{}'.format(args)))
        raise StopIteration(LuceneResponse(total=len(fieldnames), hits=fieldnames))
        yield

    def getFieldRegistry(self):
        return self._fieldRegistry

    def numDocs(self):
        raise StopIteration((yield self._client.send(path='/numDocs/')))

    def coreInfo(self):
        yield self.LuceneInfo(self)

    class LuceneInfo(object):
        def __init__(inner, self):
            inner._lucene = self
            inner.name = self._name
            inner.numDocs = self.numDocs


def luceneResponseFromDict(responseDict):
    hits = [Hit(**hit) for hit in responseDict['hits']]
    response = LuceneResponse(total=responseDict["total"], queryTime=responseDict["queryTime"], hits=hits, drilldownData=[])
    if "totalWithDuplicates" in responseDict:
        response.totalWithDuplicates = responseDict['totalWithDuplicates']
    if "drilldownData" in responseDict:
        response.drilldownData = responseDict['drilldownData']
    if "suggestions" in responseDict:
        response.suggestions = responseDict['suggestions']
    if "times" in responseDict:
        response.times = responseDict['times']
    return response

millis = lambda seconds: int(seconds * 1000) or 1 # nobody believes less than 1 millisecs

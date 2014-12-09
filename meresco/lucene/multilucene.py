## begin license ##
#
# "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
#
# Copyright (C) 2013-2014 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2013-2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
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

from time import time

from weightless.core import DeclineMessage
from meresco.core import Observable

from seecr.utils.generatorutils import generatorReturn

from org.apache.lucene.search import MatchAllDocsQuery
from org.meresco.lucene.search.join import KeySuperCollector, ScoreCollector, AggregateScoreCollector, AggregateScoreSuperCollector, ScoreSuperCollector, KeyCollector
from org.meresco.lucene.queries import KeyFilter
from java.util import ArrayList

from _lucene import millis


class MultiLucene(Observable):
    def __init__(self, defaultCore, multithreaded=True):
        Observable.__init__(self)
        self._defaultCore = defaultCore
        self._multithreaded = multithreaded

    def executeQuery(self, core=None, **kwargs):
        coreName = self._defaultCore if core is None else core
        response = yield self.any[coreName].executeQuery(**kwargs)
        generatorReturn(response)

    def executeComposedQuery(self, query):
        query.validate()
        if query.isSingleCoreQuery():
            response = yield self._sinqleQuery(query)
            generatorReturn(response)
        response = yield self._multipleCoreQuery(query)
        generatorReturn(response)

    def _sinqleQuery(self, query):
        t0 = time()
        resultCoreName = query.resultsFrom
        resultCoreQuery = query.queryFor(core=resultCoreName)
        if resultCoreQuery is None:
            resultCoreQuery = MatchAllDocsQuery()
        result = yield self.any[resultCoreName].executeQuery(
                luceneQuery=resultCoreQuery,
                facets=query.facetsFor(resultCoreName),
                filterQueries=query.filterQueriesFor(resultCoreName),
                drilldownQueries=query.drilldownQueriesFor(resultCoreName),
                **query.otherKwargs()
            )
        result.queryTime = millis(time() - t0)
        generatorReturn(result)

    def _multipleCoreQuery(self, query):
        t0 = time()
        resultCoreName = query.resultsFrom
        resultCoreKey = query.keyName(resultCoreName)
        otherCoreNames = [coreName for coreName in query.cores if coreName != resultCoreName]

        finalKeys = self._uniteFilter(query)
        for otherCoreName in otherCoreNames:
            finalKeys = self._coreQueries(otherCoreName, query, finalKeys)

        summaryFilter = None
        if finalKeys is not None:
            summaryFilter = KeyFilter(finalKeys, resultCoreKey)

        resultCoreQuery = self._luceneQueryForCore(resultCoreName, query)
        aggregateScoreCollector = self._createAggregateScoreCollector(query, resultCoreKey)
        keyCollector = self._createKeyCollector(resultCoreKey)
        result = yield self.any[resultCoreName].executeQuery(
                luceneQuery=resultCoreQuery or MatchAllDocsQuery(),
                filter=summaryFilter,
                facets=query.facetsFor(resultCoreName),
                scoreCollector=aggregateScoreCollector,
                keyCollector=keyCollector,
                **query.otherKwargs()
            )

        for otherCoreName in otherCoreNames:
            if query.facetsFor(otherCoreName):
                keyFilter = KeyFilter(keyCollector.getCollectedKeys(), query.keyName(otherCoreName))
                result.drilldownData.extend((yield self.any[otherCoreName].facets(
                    facets=query.facetsFor(otherCoreName),
                    filterQueries=query.queriesFor(otherCoreName) + query.uniteQueriesFor(otherCoreName),
                    drilldownQueries=query.drilldownQueriesFor(otherCoreName),
                    filter=keyFilter
                )))

        result.queryTime = millis(time() - t0)
        generatorReturn(result)

    def _collectKeys(self, filter, coreName, keyName, query=MatchAllDocsQuery()):
        keyCollector = self._createKeyCollector(keyName)
        self.do[coreName].search(query=query, filterQuery=filter, collector=keyCollector)
        return keyCollector.getCollectedKeys()

    def _uniteFilter(self, query):
        keys = None
        for core, q in query.unites:
            collectedKeys = self._collectKeys(q, core, query.keyName(core))
            if keys is None:
                keys = collectedKeys
            else:
                keys.union(collectedKeys)
        for core, qs in query.filterQueries:
            for q in qs:
                collectedKeys = self._collectKeys(q, core, query.keyName(core))
                if keys is None:
                    keys = collectedKeys
                else:
                    keys.intersect(collectedKeys)
        return keys

    def _coreQueries(self, coreName, query, keys):
        luceneQuery = self._luceneQueryForCore(coreName, query)
        if luceneQuery:
            collectedKeys = self._collectKeys(filter=None, coreName=coreName, keyName=query.keyName(coreName), query=luceneQuery)
            if keys:
                keys.intersect(collectedKeys)
            else:
                keys = collectedKeys
        return keys

    def _luceneQueryForCore(self, coreName, query):
        luceneQuery = query.queryFor(coreName)
        ddQueries = query.drilldownQueriesFor(coreName)
        if ddQueries:
            luceneQuery = self.call[coreName].createDrilldownQuery(luceneQuery, ddQueries)
        return luceneQuery

    def _createKeyCollector(self, keyName):
        return KeySuperCollector(keyName) if self._multithreaded else KeyCollector(keyName)

    def _createAggregateScoreCollector(self, query, keyName):
        scoreCollectors = ArrayList().of_(ScoreSuperCollector if self._multithreaded else ScoreCollector)
        for coreName in query.cores:
            rankQuery = query.rankQueryFor(coreName)
            if rankQuery:
                scoreCollector = self.call[coreName].scoreCollector(keyName=query.keyName(coreName), query=rankQuery)
                scoreCollectors.add(scoreCollector)
        constructor = AggregateScoreSuperCollector if self._multithreaded else AggregateScoreCollector
        return constructor(keyName, scoreCollectors) if scoreCollectors.size() > 0 else None

    def any_unknown(self, message, core=None, **kwargs):
        if message in ['prefixSearch', 'fieldnames', 'drilldownFieldnames']:
            core = self._defaultCore if core is None else core
            result = yield self.any[core].unknown(message=message, **kwargs)
            raise StopIteration(result)
        raise DeclineMessage()

    def coreInfo(self):
        yield self.all.coreInfo()


## begin license ##
#
# "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
#
# Copyright (C) 2013-2015 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2013-2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
# Copyright (C) 2015 Koninklijke Bibliotheek (KB) http://www.kb.nl
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
from org.meresco.lucene.search.join import KeySuperCollector, AggregateScoreSuperCollector, ScoreSuperCollector
from org.meresco.lucene.queries import KeyFilter
from org.meresco.lucene.search import JoinSortCollector, JoinSortField
from java.util import ArrayList

from _lucene import millis


class MultiLucene(Observable):
    def __init__(self, defaultCore):
        Observable.__init__(self)
        self._defaultCore = defaultCore

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
        response.info = query.infoDict()
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
        otherCoreNames = [coreName for coreName in query.cores if coreName != resultCoreName]

        finalKeys = self._uniteFilter(query)
        for otherCoreName in otherCoreNames:
            finalKeys = self._coreQueries(otherCoreName, resultCoreName, query, finalKeys)

        resultFilters = []
        for keyName, keys in finalKeys.items():
            resultFilters.append(KeyFilter(keys, keyName))

        resultCoreQuery = self._luceneQueryForCore(resultCoreName, query)
        aggregateScoreCollectors = self._createAggregateScoreCollectors(query)
        keyCollectors = dict()
        for keyName in query.keyNames(resultCoreName):
            keyCollectors[keyName] = KeySuperCollector(keyName)

        joinSortCollectors = dict()
        for i, sortKey in enumerate(query.sortKeys):
            coreName = sortKey.get('core', resultCoreName)
            if coreName != resultCoreName:
                if coreName not in joinSortCollectors:
                    joinSortCollectors[coreName] = joinSortCollector = JoinSortCollector(query.keyName(resultCoreName, coreName), query.keyName(coreName, resultCoreName))
                    self.call[coreName].search(query=MatchAllDocsQuery(), collector=joinSortCollector)
                fieldRegistry = self.call[coreName].getFieldRegistry()
                sortField = JoinSortField(sortKey['sortBy'], fieldRegistry.sortFieldType(sortKey['sortBy']), sortKey['sortDescending'], joinSortCollectors[sortKey['core']])
                sortField.setMissingValue(fieldRegistry.missingValueForSort(sortKey['sortBy'], sortKey['sortDescending']))
                query.sortKeys[i] = sortField

        result = yield self.any[resultCoreName].executeQuery(
                luceneQuery=resultCoreQuery or MatchAllDocsQuery(),
                filters=resultFilters,
                facets=query.facetsFor(resultCoreName),
                scoreCollectors=aggregateScoreCollectors,
                keyCollectors=keyCollectors.values(),
                **query.otherKwargs()
            )

        for otherCoreName in otherCoreNames:
            if query.facetsFor(otherCoreName):
                coreKey = query.keyName(resultCoreName, otherCoreName)
                keyFilter = KeyFilter(keyCollectors[coreKey].getCollectedKeys(), query.keyName(otherCoreName, resultCoreName))
                result.drilldownData.extend((yield self.any[otherCoreName].facets(
                    facets=query.facetsFor(otherCoreName),
                    filterQueries=query.queriesFor(otherCoreName) + query.otherCoreFacetFiltersFor(otherCoreName),
                    drilldownQueries=query.drilldownQueriesFor(otherCoreName),
                    filters=[keyFilter]
                )))

        result.queryTime = millis(time() - t0)
        generatorReturn(result)

    def _uniteFilter(self, query):
        keys = dict()
        for unite in query.unites:
            for uniteSpec, keyNameResult in unite.queries():
                collectedKeys = self.call[uniteSpec['core']].collectKeys(uniteSpec['query'], uniteSpec['keyName'])
                if keyNameResult not in keys:
                    keys[keyNameResult] = collectedKeys.clone()
                else:
                    keys[keyNameResult].union(collectedKeys)
        for core, qs in query.filterQueries:
            for q in qs:
                keyNameResult = query.keyName(query.resultsFrom, core)
                keyNameOther = query.keyName(core, query.resultsFrom)
                collectedKeys = self.call[core].collectKeys(q, keyNameOther)
                if keyNameResult not in keys:
                    keys[keyNameResult] = collectedKeys.clone()
                else:
                    keys[keyNameResult].intersect(collectedKeys)
        return keys

    def _coreQueries(self, coreName, otherCoreName, query, keysForKeyName):
        luceneQuery = self._luceneQueryForCore(coreName, query)
        if luceneQuery:
            collectedKeys = self.call[coreName].collectKeys(filter=None, keyName=query.keyName(coreName, otherCoreName), query=luceneQuery, cacheCollectedKeys=False)
            otherKeyName = query.keyName(otherCoreName, coreName)
            if otherKeyName in keysForKeyName:
                keysForKeyName[otherKeyName].intersect(collectedKeys)
            else:
                keysForKeyName[otherKeyName] = collectedKeys
        return keysForKeyName

    def _luceneQueryForCore(self, coreName, query):
        luceneQuery = query.queryFor(coreName)
        ddQueries = query.drilldownQueriesFor(coreName)
        if ddQueries:
            luceneQuery = self.call[coreName].createDrilldownQuery(luceneQuery, ddQueries)
        return luceneQuery

    def _createAggregateScoreCollectors(self, query):
        scoreCollectors = dict()
        for coreName in query.cores:
            resultsKeyName = query.keyName(query.resultsFrom, coreName)
            rankQuery = query.rankQueryFor(coreName)
            if rankQuery:
                scoreCollector = self.call[coreName].scoreCollector(keyName=query.keyName(coreName, query.resultsFrom), query=rankQuery)
                if resultsKeyName not in scoreCollectors:
                    scoreCollectors[resultsKeyName] = ArrayList().of_(ScoreSuperCollector)
                scoreCollectors[resultsKeyName].add(scoreCollector)
        aggregateScoreCollectors = []
        for keyName, scoreCollectorList in scoreCollectors.items():
            if scoreCollectorList.size() > 0:
                aggregateScoreCollectors.append(AggregateScoreSuperCollector(keyName, scoreCollectorList))
        return aggregateScoreCollectors

    def any_unknown(self, message, **kwargs):
        if message in ['prefixSearch', 'fieldnames', 'drilldownFieldnames']:
            core = kwargs.get('core')
            if core is None:
                core = self._defaultCore
            result = yield self.any[core].unknown(message=message, **kwargs)
            raise StopIteration(result)
        raise DeclineMessage()

    def coreInfo(self):
        yield self.all.coreInfo()


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

from org.apache.lucene.search import MatchAllDocsQuery, BooleanClause
from org.meresco.lucene.search.join import KeyCollectorCache, ScoreCollector, AggregateScoreCollector, AggregateScoreSuperCollector, ScoreSuperCollector, KeyFilterCache
from org.meresco.lucene.queries import KeyBooleanFilter
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

    def collectKeys(self, query, coreName, keyName):
        if self._multithreaded:
            keyCollector = KeyCollectorCache.createSuper(query, keyName)
        else:
            keyCollector = KeyCollectorCache.create(query, keyName)
        self.do[coreName].search(query=query, collector=keyCollector)
        return keyCollector

    def collectUniteKeyCollectors(self, query):
        for d in query.unites:
            yield self.collectKeys(d['query'], d['core'], query.keyName(d['core']))

    def orCollectors(self, collectors, keyName):
        booleanFilter = None
        for keyCollector in collectors:
            if not booleanFilter:
                booleanFilter = KeyBooleanFilter()
            booleanFilter.add(KeyFilterCache.create(keyCollector, keyName), BooleanClause.Occur.SHOULD)
        return booleanFilter

    def andQueries(self, coreQuerySpecs, filterKeyName, booleanFilter):
        for coreName, keyName, luceneQuery, filterQueries, drilldownQueries in coreQuerySpecs:
            if drilldownQueries:
                luceneQuery = self.call[coreName].createDrilldownQuery(luceneQuery, drilldownQueries)
            for q in [luceneQuery] + filterQueries:
                if not q:
                    continue
                if not booleanFilter:
                    booleanFilter = KeyBooleanFilter()
                keyCollector = self.collectKeys(q, coreName, keyName)
                booleanFilter.add(KeyFilterCache.create(keyCollector, filterKeyName), BooleanClause.Occur.MUST)
        return booleanFilter

    def executeComposedQuery(self, query):
        query.validate()
        if query.isSingleCoreQuery():
            response = yield self._sinqleQuery(query)
            generatorReturn(response)
        response = yield self._multipleCoreQuery(query)
        generatorReturn(response)

    def _multipleCoreQuery(self, query):
        t0 = time()
        resultCoreName = query.resultsFrom
        otherCoreNames = [coreName for coreName in query.cores if coreName != resultCoreName]
        coreBaseFilters = {}

        resultCoreKey = query.keyName(resultCoreName)

        resultCoreBaseFilter = None
        if query.unites:
            unitesKeyCollectors = list(self.collectUniteKeyCollectors(query))
            resultCoreBaseFilter = self.orCollectors(unitesKeyCollectors, resultCoreKey)

            uniteOtherCore = [d['core'] for d in query.unites if d['core'] != resultCoreName][0]
            coreBaseFilters[uniteOtherCore] = self.orCollectors(unitesKeyCollectors, query.keyName(uniteOtherCore))

        coreQuerySpecs = []
        for otherCoreName in otherCoreNames:
            coreQuerySpecs.append((otherCoreName, query.keyName(otherCoreName), query.queryFor(otherCoreName), query.filterQueriesFor(otherCoreName), query.drilldownQueriesFor(otherCoreName)))
        resultCoreIntermediateFilter = self.andQueries(coreQuerySpecs, resultCoreKey, resultCoreBaseFilter)

        drilldownData = []
        resultCoreQuery = query.queryFor(resultCoreName)
        if resultCoreQuery is None:
            resultCoreQuery = MatchAllDocsQuery()
        for otherCoreName in otherCoreNames:
            if query.facetsFor(otherCoreName):
                otherCoreKey = query.keyName(otherCoreName)
                otherCoreIntermediateFilter = self.andQueries([(otherCoreName, otherCoreKey, query.queryFor(otherCoreName), query.filterQueriesFor(otherCoreName), query.drilldownQueriesFor(otherCoreName))], otherCoreKey, coreBaseFilters.get(otherCoreName))

                coreQuerySpecs = [(resultCoreName, resultCoreKey, resultCoreQuery, query.filterQueriesFor(resultCoreName), query.drilldownQueriesFor(resultCoreName))]
                for name in otherCoreNames:
                    if name != otherCoreName:
                        coreQuerySpecs.append((name, query.keyName(name), query.queryFor(name), query.filterQueriesFor(name), query.drilldownQueriesFor(name)))
                otherCoreFinalFilter = self.andQueries(coreQuerySpecs, otherCoreKey, otherCoreIntermediateFilter)
                drilldownData.extend((yield self.any[otherCoreName].facets(
                        filterQueries=query.queriesFor(otherCoreName) + query.uniteQueriesFor(otherCoreName),
                        drilldownQueries=query.drilldownQueriesFor(otherCoreName),
                        filter=otherCoreFinalFilter,
                        facets=query.facetsFor(otherCoreName)
                    )))

        scoreCollectors = ArrayList().of_(ScoreSuperCollector if self._multithreaded else ScoreCollector)
        for coreName in [resultCoreName] + otherCoreNames:
            rankQuery = query.rankQueryFor(coreName)
            if rankQuery:
                scoreCollector = self.call[coreName].scoreCollector(keyName=query.keyName(coreName), query=rankQuery)
                scoreCollectors.add(scoreCollector)
        constructor = AggregateScoreSuperCollector if self._multithreaded else AggregateScoreCollector
        aggregateScoreCollector = constructor(resultCoreKey, scoreCollectors) if scoreCollectors.size() > 0 else None

        # KeyCollectorCache.printStats()
        # KeyFilterCache.printStats();

        result = yield self.any[resultCoreName].executeQuery(
                luceneQuery=resultCoreQuery,
                filter=resultCoreIntermediateFilter,
                facets=query.facetsFor(resultCoreName),
                filterQueries=query.filterQueriesFor(resultCoreName),
                drilldownQueries=query.drilldownQueriesFor(resultCoreName),
                scoreCollector=aggregateScoreCollector,
                **query.otherKwargs()
            )

        result.drilldownData.extend(drilldownData)
        result.queryTime = millis(time() - t0)
        generatorReturn(result)


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

    def any_unknown(self, message, core=None, **kwargs):
        if message in ['prefixSearch', 'fieldnames', 'drilldownFieldnames']:
            core = self._defaultCore if core is None else core
            result = yield self.any[core].unknown(message=message, **kwargs)
            raise StopIteration(result)
        raise DeclineMessage()

    def coreInfo(self):
        yield self.all.coreInfo()


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

from seecr.utils.generatorutils import consume, generatorReturn

from org.apache.lucene.search import MatchAllDocsQuery, BooleanClause
from org.meresco.lucene import CachingKeyCollector, KeyBooleanFilter

from _lucene import millis


class MultiLucene(Observable):
    def __init__(self, defaultCore):
        Observable.__init__(self)
        self._defaultCore = defaultCore

    def executeQuery(self, core=None, **kwargs):
        coreName = self._defaultCore if core is None else core
        response = yield self.any[coreName].executeQuery(**kwargs)
        generatorReturn(response)

    def collectKeys(self, query, coreName, keyName):
        keyCollector = CachingKeyCollector.create(query, keyName)
        consume(self.any[coreName].search(query=query, collector=keyCollector))
        return keyCollector

    def collectUniteKeyCollectors(self, query):
        for d in query.unites:
            yield self.collectKeys(d['query'], d['core'], query.keyName(d['core']))

    def orCollectors(self, collectors, keyName):
        booleanFilter = None
        for keyCollector in collectors:
            if not booleanFilter:
                booleanFilter = KeyBooleanFilter()
            booleanFilter.add(keyCollector.getFilter(keyName), BooleanClause.Occur.SHOULD)
        return booleanFilter

    def andQueries(self, coreQuerySpecs, filterKeyName, booleanFilter):
        for coreName, keyName, queries in coreQuerySpecs:
            for q in queries:
                if not booleanFilter:
                    booleanFilter = KeyBooleanFilter()
                keyCollector = CachingKeyCollector.create(q, keyName)
                consume(self.any[coreName].search(query=q, collector=keyCollector))
                booleanFilter.add(keyCollector.getFilter(filterKeyName), BooleanClause.Occur.MUST)
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
        otherCoreSpecs = {}
        for coreName in query.cores:
            if coreName == resultCoreName:
                continue
            otherCoreSpecs[coreName] = dict(key=query.keyName(coreName))

        resultMatchKeyName = query.keyName(resultCoreName)

        resultCoreBaseFilter = None
        if query.unites:
            unitesKeyCollectors = list(self.collectUniteKeyCollectors(query))
            resultCoreBaseFilter = self.orCollectors(unitesKeyCollectors, resultMatchKeyName)

            uniteOtherCore = [d['core'] for d in query.unites if d['core'] != resultCoreName][0]
            otherCoreSpecs[uniteOtherCore]['baseFilter'] = self.orCollectors(unitesKeyCollectors, otherCoreSpecs[uniteOtherCore]['key'])

        coreQuerySpecs = []
        for otherCoreName, otherCoreSpec in otherCoreSpecs.items():
            coreQuerySpecs.append((otherCoreName, otherCoreSpec["key"], query.queriesFor(otherCoreName)))
        resultCoreIntermediateFilter = self.andQueries(coreQuerySpecs, resultMatchKeyName, resultCoreBaseFilter)

        drilldownData = []
        resultCoreQueries = query.queriesFor(resultCoreName)
        if not resultCoreQueries:
            resultCoreQueries = [MatchAllDocsQuery()]
        for otherCoreName, otherCoreSpec in otherCoreSpecs.items():
            if query.facetsFor(otherCoreName):
                otherCoreBaseFilter = otherCoreSpec.get('baseFilter')
                otherCoreIntermediateFilter = self.andQueries([(otherCoreName, otherCoreSpec["key"], query.queriesFor(otherCoreName))], otherCoreSpec["key"], otherCoreBaseFilter)

                coreQuerySpecs = [(resultCoreName, resultMatchKeyName, resultCoreQueries)]
                for name, spec in otherCoreSpecs.items():
                    if name != otherCoreName:
                        coreQuerySpecs.append((name, spec["key"], query.queriesFor(name)))
                otherCoreFinalFilter = self.andQueries(coreQuerySpecs, otherCoreSpec['key'], otherCoreIntermediateFilter)
                drilldownData.extend((yield self.any[otherCoreName].facets(
                        filterQueries=query.queriesFor(otherCoreName) + query.uniteQueriesFor(otherCoreName),
                        filter=otherCoreFinalFilter,
                        facets=query.facetsFor(otherCoreName)
                    )))

        resultCoreQuery = query.queryFor(core=resultCoreName)
        if resultCoreQuery is None:
            resultCoreQuery = MatchAllDocsQuery()
        result = yield self.any[resultCoreName].executeQuery(
                luceneQuery=resultCoreQuery,
                filter=resultCoreIntermediateFilter,
                facets=query.facetsFor(resultCoreName),
                filterQueries=query.filterQueriesFor(resultCoreName),
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
                **query.otherKwargs()
            )
        result.queryTime = millis(time() - t0)
        generatorReturn(result)

    def any_unknown(self, message, core=None, **kwargs):
        if message in ['prefixSearch', 'fieldnames']:
            core = self._defaultCore if core is None else core
            result = yield self.any[core].unknown(message=message, **kwargs)
            raise StopIteration(result)
        raise DeclineMessage()

    def coreInfo(self):
        yield self.all.coreInfo()


class KeySetWrap(object):
    def __init__(self):
        self.keySet = None

    def intersect(self, otherKeySet):
        if self.keySet is None:
            self.keySet = otherKeySet
        else:
            getattr(self.keySet, "and")(otherKeySet)

    def union(self, otherKeySet):
        if self.keySet is None:
            self.keySet = otherKeySet
        else:
            getattr(self.keySet, "or")(otherKeySet)


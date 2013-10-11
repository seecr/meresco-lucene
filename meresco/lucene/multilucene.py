## begin license ##
#
# "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
#
# Copyright (C) 2013 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2013 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
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

from meresco.core import Observable
from org.apache.lucene.search import MatchAllDocsQuery
from weightless.core import DeclineMessage
from _lucene import millis
from time import time
from org.meresco.lucene import KeyCollector, KeyFilterCollector, CachingKeyCollector
from seecr.utils.generatorutils import consume, generatorReturn


class MultiLucene(Observable):
    def __init__(self, defaultCore):
        Observable.__init__(self)
        self._defaultCore = defaultCore

    def executeQuery(self, core=None, **kwargs):
        coreName = self._defaultCore if core is None else core
        response = yield self.any[coreName].executeQuery(**kwargs)
        generatorReturn(response)

    def orAllUnites(self, query, keyName):
        keySetWrap = KeySetWrap()
        for coreName, coreKeyName, uniteQuery in query.unites():
            keyCollector = CachingKeyCollector.create(uniteQuery, coreKeyName)
            consume(self.any[coreName].search(query=uniteQuery, collector=keyCollector))
            keySetWrap.union(keyCollector.getCollectedKeys())
            #booleanFiler.addFilter(keyCollector.getFilter(keyName, OR))
        return keySetWrap

    def andAllQueries(self, queries, coreName, keyName, keyFilter):
        for q in queries:
            keyCollector = CachingKeyCollector.create(q, keyName)
            consume(self.any[coreName].search(query=q, collector=keyCollector))
            keyFilter.intersect(keyCollector.getCollectedKeys())
        return keyFilter

    def executeComposedQuery(self, query):
        query.validate()
        if query.isSingleCoreQuery():
            response = yield self._sinqleQuery(query)
            generatorReturn(response)
        t0 = time()

        resultCoreName, otherCoreName = query.cores()
        resultMatchKeyName, otherMatchKeyName = query.keyNames(resultCoreName, otherCoreName)

        otherCoreBaseFilter = self.orAllUnites(query, otherMatchKeyName)
        otherCoreIntermediateFilter = self.andAllQueries(query.queriesFor(otherCoreName), otherCoreName, otherMatchKeyName, otherCoreBaseFilter)

        drilldownData = []
        otherCoreFinalFilter = otherCoreIntermediateFilter
        if query.facetsFor(otherCoreName):
            resultCoreQueries = query.queriesFor(resultCoreName)
            if not resultCoreQueries:
                resultCoreQueries = [MatchAllDocsQuery()]

            otherCoreFinalFilter = self.andAllQueries(resultCoreQueries, resultCoreName, resultMatchKeyName, otherCoreIntermediateFilter)

            drilldownData.extend((yield self.any[otherCoreName].facets(
                    filterQueries=query.queriesFor(otherCoreName) + query.uniteQueriesFor(otherCoreName),
                    filterCollector=KeyFilterCollector(otherCoreFinalFilter.keySet, otherMatchKeyName),
                    facets=query.facetsFor(otherCoreName)
                )))

        resultMatchKeyFilterCollector = KeyFilterCollector(otherCoreFinalFilter.keySet, resultMatchKeyName)
        resultCoreQuery = query.queryFor(core=resultCoreName)
        if resultCoreQuery is None:
            resultCoreQuery = MatchAllDocsQuery()
        result = yield self.any[resultCoreName].executeQuery(
                luceneQuery=resultCoreQuery,
                filterCollector=resultMatchKeyFilterCollector,
                facets=query.facetsFor(resultCoreName),
                filterQueries=query.filterQueriesFor(resultCoreName),
                **query.otherKwargs()
            )

        result.drilldownData.extend(drilldownData)
        result.queryTime = millis(time() - t0)
        generatorReturn(result)

    def _sinqleQuery(self, query):
        t0 = time()
        resultCoreName = query.cores()[0]
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


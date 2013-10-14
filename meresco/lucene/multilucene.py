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
from org.apache.lucene.search import MatchAllDocsQuery, BooleanClause
from org.apache.lucene.queries import BooleanFilter
from weightless.core import DeclineMessage
from _lucene import millis
from time import time
from org.meresco.lucene import KeyCollector, KeyFilterCollector, CachingKeyCollector, MyBooleanFilter
from seecr.utils.generatorutils import consume, generatorReturn


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
        for coreName, coreKeyName, uniteQuery in query.unites():
            yield self.collectKeys(uniteQuery, coreName, coreKeyName)

    def orCollectors(self, collectors, keyName):
        keySetWrap = KeySetWrap()
        filters = []
        for keyCollector in collectors:
            filters.append(keyCollector.getFilter(keyName))
        if filters:
            booleanFilter = MyBooleanFilter()
            [booleanFilter.add(f, BooleanClause.Occur.SHOULD) for f in filters]
            return booleanFilter

    def andAllQueries(self, queries, coreName, keyName, filterKeyName, booleanFilter):
        filters = []
        for q in queries:
            keyCollector = CachingKeyCollector.create(q, keyName)
            consume(self.any[coreName].search(query=q, collector=keyCollector))
            filters.append(keyCollector.getFilter(filterKeyName))
        if filters:
            if not booleanFilter:
                booleanFilter = MyBooleanFilter()
            [booleanFilter.add(f, BooleanClause.Occur.MUST) for f in filters]
        return booleanFilter

    def executeComposedQuery(self, query):
        query.validate()
        if query.isSingleCoreQuery():
            response = yield self._sinqleQuery(query)
            generatorReturn(response)
        t0 = time()

        resultCoreName, otherCoreName = query.cores()
        resultMatchKeyName, otherMatchKeyName = query.keyNames(resultCoreName, otherCoreName)

        unitesKeyCollectors = list(self.collectUniteKeyCollectors(query))
        t1 = time()
        otherCoreBaseFilter = self.orCollectors(unitesKeyCollectors, otherMatchKeyName)
        t2 = time()
        resultCoreBaseFilter = self.orCollectors(unitesKeyCollectors, resultMatchKeyName)

        t3 = time()
        otherCoreIntermediateFilter = self.andAllQueries(query.queriesFor(otherCoreName), otherCoreName, otherMatchKeyName, otherMatchKeyName, otherCoreBaseFilter)
        t4 = time()
        resultCoreIntermediateFilter = self.andAllQueries(query.queriesFor(otherCoreName), otherCoreName, otherMatchKeyName, resultMatchKeyName, resultCoreBaseFilter)
        t5 = time()
        print "t1=" + str(t1-t0) + "; t2=" + str(t2-t1) + "; t3=" + str(t3-t2) + "; t4=" + str(t4-t3) + "; t5=" + str(t5-t4)
        drilldownData = []
        if query.facetsFor(otherCoreName):
            resultCoreQueries = query.queriesFor(resultCoreName)
            if not resultCoreQueries:
                resultCoreQueries = [MatchAllDocsQuery()]

            t6 = time()
            otherCoreFinalFilter = self.andAllQueries(resultCoreQueries, resultCoreName, resultMatchKeyName, otherMatchKeyName, otherCoreIntermediateFilter)
            t7 = time()
            print query.queriesFor(otherCoreName) + query.uniteQueriesFor(otherCoreName)
            drilldownData.extend((yield self.any[otherCoreName].facets(
                    filterQueries=query.queriesFor(otherCoreName) + query.uniteQueriesFor(otherCoreName),
                    filter=otherCoreFinalFilter,
                    facets=query.facetsFor(otherCoreName)
                )))
            t8 = time()
            print "t6=" + str(t6-t5) + "; t7=" + str(t7-t6) + "; t8=" + str(t8-t7)

        t9 = time()
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
        t10 = time()
        print "t10=" + str(t10-t9)

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


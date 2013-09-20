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
from org.meresco.lucene import KeyCollector, KeyFilterCollector
from cache import KeyCollectorCache
from seecr.utils.generatorutils import consume, generatorReturn

class MultiLucene(Observable):
    def __init__(self, defaultCore):
        Observable.__init__(self)
        self._defaultCore = defaultCore
        self._collectorCache = KeyCollectorCache(createCollectorFunction=lambda core: KeyCollector(core))

    def executeQuery(self, core=None, **kwargs):
        coreName = self._defaultCore if core is None else core
        response = yield self.any[coreName].executeQuery(**kwargs)
        generatorReturn(response)

    def executeComposedQuery(self, query):
        query.validate()
        t0 = time()

        primaryCoreName, foreignCoreName = query.cores()
        primaryKeyName, foreignKeyName = query.keyNames(primaryCoreName, foreignCoreName)
        keySetWrap = KeySetWrap()
        for coreName, coreKeyName, unite in query.unites():
            uniteKeyCollector = KeyCollector(coreKeyName)
            consume(self.any[coreName].search(query=unite, collector=uniteKeyCollector))
            keySetWrap.union(uniteKeyCollector.getKeySet())

        for q in query.queriesFor(foreignCoreName):
            foreignKeyCollector = KeyCollector(foreignKeyName)
            consume(self.any[foreignCoreName].search(query=q, collector=foreignKeyCollector))
            keySetWrap.intersect(foreignKeyCollector.getKeySet())
        if query.facetsFor(foreignCoreName):
            primaryQueries = query.queriesFor(primaryCoreName)
            if not primaryQueries:
                primaryQueries = [MatchAllDocsQuery()]
            for q in primaryQueries:
                primaryKeyCollector = KeyCollector(primaryKeyName)
                consume(self.any[primaryCoreName].search(query=q, collector=primaryKeyCollector))
                keySetWrap.intersect(primaryKeyCollector.getKeySet())
        primaryKeyFilterCollector = KeyFilterCollector(keySetWrap.keySet, primaryKeyName)
        primaryQuery = query.queryFor(core=primaryCoreName)
        if primaryQuery is None:
            primaryQuery = MatchAllDocsQuery()
        primaryResponse = yield self.any[primaryCoreName].executeQuery(
                luceneQuery=primaryQuery,
                filterCollector=primaryKeyFilterCollector,
                facets=query.facetsFor(primaryCoreName),
                filterQueries=query.filterQueriesFor(primaryCoreName),
                **query.otherKwargs()
            )

        if query.facetsFor(foreignCoreName):
            foreignDrilldownData = yield self.any[foreignCoreName].facets(
                    filterCollector=KeyFilterCollector(keySetWrap.keySet, foreignKeyName),
                    facets=query.facetsFor(foreignCoreName)
                )
            primaryResponse.drilldownData.extend(foreignDrilldownData)

        primaryResponse.queryTime = millis(time() - t0)
        generatorReturn(primaryResponse)

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
            self.keySet.intersect(otherKeySet)

    def union(self, otherKeySet):
        if self.keySet is None:
            self.keySet = otherKeySet
        else:
            self.keySet.union(otherKeySet)


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
from itertools import groupby
from org.apache.lucene.search import MatchAllDocsQuery
from weightless.core import DeclineMessage
from _lucene import millis
from time import time
from org.meresco.lucene import HashCollector, HashCollectorFilter
from weightless.core import compose

class MultiLucene(Observable):
    def __init__(self, defaultCore):
        Observable.__init__(self)
        self._defaultCore = defaultCore

    def executeQuery(self, luceneQuery=None, core=None, joinQueries=None, joinFacets=None, joins=None, **kwargs):
        t0 = time()
        core = self._defaultCore if core is None else core
        joinQueries = joinQueries or {}
        joinFacets = joinFacets or {}

        if not (joinQueries or joinFacets):
            response = yield self.any[core].executeQuery(luceneQuery=luceneQuery, **kwargs)
            raise StopIteration(response)

        filterCollector = None
        filterCollector = HashCollector(joins[core])
        state = compose(self.any[core].executeQueryGenerator(luceneQuery=luceneQuery, filterCollector=filterCollector, **kwargs))
        state.next()

        joinResults = []
        for joinCore, joinQuery in joinQueries.items():
            joinResults.append(self.call[joinCore].executeJoinQuery(luceneQuery=joinQuery, filterCollector=HashCollectorFilter(filterCollector, joins[joinCore]), facets=joinFacets.get(joinCore)))
        for joinCore, joinFacet in joinFacets.items():
            if joinCore in joinQueries:
                continue
            joinResults.append(self.call[joinCore].executeJoinQuery(luceneQuery=MatchAllDocsQuery(), filterCollector=HashCollectorFilter(filterCollector, joins[joinCore]), facets=joinFacet))

        filterCollector.finishCollecting();

        response = state.next()

        if joinFacets and not hasattr(response, "drilldownData"):
            response.drilldownData = []

        for joinResult in joinResults:
            if joinResult:
                response.drilldownData.extend(joinResult)

        response.queryTime = millis(time() - t0)
        raise StopIteration(response)

    def any_unknown(self, message, core=None, **kwargs):
        if message in ['prefixSearch', 'fieldnames']:
            core = self._defaultCore if core is None else core
            result = yield self.any[core].unknown(message=message, **kwargs)
            raise StopIteration(result)
        raise DeclineMessage()

    def coreInfo(self):
        yield self.all.coreInfo()

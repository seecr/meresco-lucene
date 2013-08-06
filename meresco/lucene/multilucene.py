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
from org.apache.lucene.search.join import TermsCollector
from weightless.core import DeclineMessage
from _lucene import millis
from filtercache import FilterCache
from time import time


class MultiLucene(Observable):
    def __init__(self, defaultCore):
        Observable.__init__(self)
        self._defaultCore = defaultCore
        self._filterCache = FilterCache(compareQueryFunction=lambda q1, q2: q1 == q2, createFilterFunction=self._joinFilterCreate)

    def executeQuery(self, luceneQuery=None, core=None, joinQueries=None, joinFacets=None, **kwargs):
        t0 = time()
        core = self._defaultCore if core is None else core
        filters = []
        if joinQueries is not None:
            for joinQuery in joinQueries:
                filters.append(self._filterCache.getFilter(query=joinQuery))
        collectors = None
        if joinFacets:
            collectors = {}
            for toField in set([joinFacet['toField'] for joinFacet in joinFacets]):
                collectors['joinFacet.field.%s' % toField] = self._createJoinCollector(toField)
        response = yield self.any[core].executeQuery(luceneQuery=luceneQuery, filters=filters, collectors=collectors, **kwargs)
        if joinFacets:
            if not hasattr(response, "drilldownData"):
                response.drilldownData = []
            for joinFacet in self.groupJoinFacets(joinFacets):
                response.drilldownData.extend(
                    self.call[joinFacet['core']].joinFacet(
                        termsCollector=collectors['joinFacet.field.%s' % joinFacet['toField']],
                        fromField=joinFacet['fromField'],
                        facets=joinFacet['facets'],
                    )
                )
        response.queryTime = millis(time() - t0)
        raise StopIteration(response)

    def any_unknown(self, message, core=None, **kwargs):
        if message in ['prefixSearch', 'fieldnames']:
            core = self._defaultCore if core is None else core
            result = yield self.any[core].unknown(message=message, **kwargs)
            raise StopIteration(result)
        raise DeclineMessage()

    def _createJoinCollector(self, toField):
        multipleValuesPerDocument = False
        return TermsCollector.create(toField, multipleValuesPerDocument)

    def coreInfo(self):
        yield self.all.coreInfo()

    def _joinQueryCreate(self, q):
        return self.call[q['core']].createJoinFilter(q['luceneQuery'], fromField=q['fromField'], toField=q['toField'])

    def _joinQueryCompare(self, q1, q2):
        return q1['core'] == q2['core'] and \
            q1['fromField'] == q2['fromField'] and \
            q1['toField'] == q2['toField'] and \
            q1['luceneQuery'].equals(q2['luceneQuery'])

    @staticmethod
    def groupJoinFacets(separateJoinFacets):
        groupedJoinFacets = groupby(
                separateJoinFacets,
                lambda jf:dict((key, jf[key]) for key in ['core', 'fromField', 'toField']
            ))
        return [dict(joinFacetPart, facets=[jf['facet'] for jf in joinFacets]) \
            for joinFacetPart, joinFacets in groupedJoinFacets]


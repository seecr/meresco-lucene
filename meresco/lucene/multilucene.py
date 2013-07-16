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

class MultiLucene(Observable):
    def __init__(self, defaultCore):
        Observable.__init__(self)
        self._defaultCore = defaultCore

    def executeQuery(self, luceneQuery=None, core=None, joinQueries=None, joinFacets=None, **kwargs):
        core = self._defaultCore if core is None else core
        filters = None
        if joinQueries is not None:
            filters = []
            for joinQuery in joinQueries:
                joinFilter = self.call[joinQuery['core']].createJoinFilter(joinQuery['luceneQuery'], fromField=joinQuery['fromField'], toField=joinQuery['toField'])
                filters.append(joinFilter)
        collectors = None
        if joinFacets:
            collectors = {}
            for toField in set([joinFacet['toField'] for joinFacet in joinFacets]):
                collectors['joinFacet.field.%s' % toField] = self._createJoinCollector(toField)
        result = yield self.any[core].executeQuery(luceneQuery=luceneQuery, filters=filters, collectors=collectors, **kwargs)
        if joinFacets:
            if not hasattr(result, "drilldownData"):
                result.drilldownData = []
            for joinFacet in self.groupJoinFacets(joinFacets):
                result.drilldownData.extend(
                    self.call[joinFacet['core']].joinFacet(
                        termsCollector=collectors['joinFacet.field.%s' % joinFacet['toField']],
                        fromField=joinFacet['fromField'],
                        facets=joinFacet['facets'],
                    )
                )
        raise StopIteration(result)

    def _createJoinCollector(self, toField):
        multipleValuesPerDocument = False
        return TermsCollector.create(toField, multipleValuesPerDocument)

    @staticmethod
    def groupJoinFacets(separateJoinFacets):
        groupedJoinFacets = groupby(
                separateJoinFacets,
                lambda jf:dict((key, jf[key]) for key in ['core', 'fromField', 'toField']
            ))
        return [dict(joinFacetPart, facets=[jf['facet'] for jf in joinFacets]) \
            for joinFacetPart, joinFacets in groupedJoinFacets]

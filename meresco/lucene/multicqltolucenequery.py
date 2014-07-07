## begin license ##
#
# "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
#
# Copyright (C) 2014 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
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

from meresco.core import Transparent
from seecr.utils.generatorutils import generatorReturn

class MultiCqlToLuceneQuery(Transparent):

    def __init__(self, defaultCore, coreToCqlLuceneQueries, **kwargs):
        super(MultiCqlToLuceneQuery, self).__init__(**kwargs)
        self._defaultCore = defaultCore
        self._converts = {}
        for core, cqlToLuceneQuery in coreToCqlLuceneQueries.items():
            self._converts[core] = cqlToLuceneQuery._convert

    def executeQuery(self, cqlAbstractSyntaxTree, core=None, filterQueries=None, **kwargs):
        if core is None:
            core = self._defaultCore
        convertMethod = self._converts[core]
        if filterQueries:
            filterQueries = [convertMethod(ast) for ast in filterQueries]
        response = yield self.any.executeQuery(core=core, luceneQuery=convertMethod(cqlAbstractSyntaxTree), filterQueries=filterQueries, **kwargs)
        generatorReturn(response)

    def executeComposedQuery(self, query):
        query.convertWith(**self._converts)
        response = yield self.any.executeComposedQuery(query=query)
        generatorReturn(response)

## begin license ##
#
# "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
#
# Copyright (C) 2014-2016 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
# Copyright (C) 2015-2016 Koninklijke Bibliotheek (KB) http://www.kb.nl
# Copyright (C) 2016 Stichting Kennisnet http://www.kennisnet.nl
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

from cqlparser.cqltoexpression import cqlToExpression

from meresco.core import Transparent


class AdapterToLuceneQuery(Transparent):
    def __init__(self, defaultCore, coreConverters, **kwargs):
        Transparent.__init__(self, **kwargs)
        self._defaultCore = defaultCore
        self._converts = {}
        for core, convert in coreConverters.items():
            self._converts[core] = convert

    def executeQuery(self, query=None, core=None, filterQueries=None, **kwargs):
        if 'cqlAbstractSyntaxTree' in kwargs:
            query = kwargs.pop('cqlAbstractSyntaxTree')
        expression = cqlToExpression(query)
        if core is None:
            core = self._defaultCore
        convertMethod = self._converts[core]
        if filterQueries:
            filterQueries = [convertMethod(cqlToExpression(ast)) for ast in filterQueries]
        response = yield self.any.executeQuery(core=core, luceneQuery=convertMethod(expression), filterQueries=filterQueries, **kwargs)
        raise StopIteration(response)

    def executeComposedQuery(self, query):
        query.convertWith(**self._converts)
        response = yield self.any.executeComposedQuery(query=query)
        raise StopIteration(response)

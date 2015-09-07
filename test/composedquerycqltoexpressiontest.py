## begin license ##
#
# "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
#
# Copyright (C) 2015 Koninklijke Bibliotheek (KB) http://www.kb.nl
# Copyright (C) 2015 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2015 Stichting Kennisnet http://www.kennisnet.nl
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

from seecr.test import SeecrTestCase, CallTrace
from cqlparser import parseString as parseCQL
from meresco.lucene import ComposedQuery
from weightless.core import consume
from meresco.lucene.composedquerycqltoexpression import ComposedQueryCqlToExpression
from cqlparser.cqltoexpression import QueryExpression

class ComposedQueryCqlToExpressionTest(SeecrTestCase):

    def testConvert(self):
        cq = ComposedQuery('core', parseCQL('field=value'))
        cq.addFilterQuery('core', parseCQL('filter=value'))
        convertor = ComposedQueryCqlToExpression()
        observer = CallTrace(emptyGeneratorMethods=['executeComposedQuery'])
        convertor.addObserver(observer)

        consume(convertor.executeComposedQuery(cq))
        self.assertEqual(['executeComposedQuery'], observer.calledMethodNames())
        cq2 = observer.calledMethods[0].kwargs['query']
        self.assertEqual(QueryExpression.searchterm(index='field', relation='=', term='value'), cq2.queryFor('core'))
        self.assertEqual(QueryExpression.searchterm(index='filter', relation='=', term='value'), cq2.filterQueriesFor('core')[0])

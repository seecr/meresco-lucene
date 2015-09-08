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

from seecr.test import SeecrTestCase
from meresco.lucene.converttofilterquery import ConvertToFilterQuery
from cqlparser import cqlToExpression

class ExtractFilterQueriesTest(SeecrTestCase):

    def setUp(self):
        super(ExtractFilterQueriesTest, self).setUp()
        self.convertToFilterQuery = ConvertToFilterQuery(['core1', 'core2'])
        self.convert = self.convertToFilterQuery.convert

    def testDoNothingForStandardQuery(self):
        query, filters = self.convert(cqlToExpression('field=value AND aap=noot'), 'core1')
        self.assertEqual(cqlToExpression('field=value AND aap=noot'), query)
        self.assertEqual({}, filters)

    def testOtherCoreQueryAtLast(self):
        query, filters = self.convert(cqlToExpression('field=value AND core2.f=v'), 'core1')
        self.assertEqual(cqlToExpression('field=value'), query)
        self.assertEqual({'core2': [cqlToExpression('f=v')]}, filters)

    def testOtherCoreQueryAtFirst(self):
        query, filters = self.convert(cqlToExpression('core2.f=v AND field=value'), 'core1')
        self.assertEqual(cqlToExpression('field=value'), query)
        self.assertEqual({'core2': [cqlToExpression('f=v')]}, filters)

    def testOtherCoreQueryWithBracesAtFirst(self):
        query, filters = self.convert(cqlToExpression('(core2.f=v) AND field=value'), 'core1')
        self.assertEqual(cqlToExpression('field=value'), query)
        self.assertEqual({'core2': [cqlToExpression('f=v')]}, filters)

    def testOtherCoreQueryWithMultipleOrClausesAtFirst(self):
        query, filters = self.convert(cqlToExpression('(core2.f=v OR core2.f=x) AND field=value'), 'core1')
        self.assertEqual(cqlToExpression('field=value'), query)
        self.assertEqual({'core2': [cqlToExpression('f=v OR f=x')]}, filters)

    def testOtherCoreQueryWithMultipleAndClausesAtFirst(self):
        query, filters = self.convert(cqlToExpression('core2.f=v AND core2.y=x AND field=value'), 'core1')
        self.assertEqual(cqlToExpression('field=value'), query)
        self.assertEqual({'core2': [cqlToExpression('f=v'), cqlToExpression('y=x')]}, filters)

    def testOtherCoreQueryWithMultipleAndClausesAtLast(self):
        query, filters = self.convert(cqlToExpression('field=value AND core2.f=v AND core2.y=x'), 'core1')
        self.assertEqual(cqlToExpression('field=value'), query)
        self.assertEqual({'core2': [cqlToExpression('f=v'), cqlToExpression('y=x')]}, filters)

    def testFieldWithDot(self):
        query, filters = self.convert(cqlToExpression('f=v AND fie.ld=value'), 'core1')
        self.assertEqual(cqlToExpression('f=v AND fie.ld=value'), query)
        self.assertEqual({}, filters)

    def testOtherFilterAndBeginAndEndOfQuery(self):
        query, filters = self.convert(cqlToExpression('core2.a=b AND f=v AND core2.b=c'), 'core1')
        self.assertEqual(cqlToExpression('f=v'), query)
        self.assertEqual({'core2': [cqlToExpression('a=b'), cqlToExpression('b=c')]}, filters)

    def testOtherFilterWithORIsIgnored(self):
        query, filters = self.convert(cqlToExpression('core2.a=b OR f=v'), 'core1')
        self.assertEqual(cqlToExpression('core2.a=b OR f=v'), query)
        self.assertEqual({}, filters)

    def testCoreQueryInOtherCore(self):
        query, filters = self.convert(cqlToExpression('core2.a=b'), 'core1')
        self.assertEqual(None, query)
        self.assertEqual({'core2': [cqlToExpression('a=b')]}, filters)

    def testCoreAndQueryInOtherCore(self):
        query, filters = self.convert(cqlToExpression('core2.a=b AND core2.x=y'), 'core1')
        self.assertEqual(None, query)
        self.assertEqual({'core2': [cqlToExpression('a=b'), cqlToExpression('x=y')]}, filters)

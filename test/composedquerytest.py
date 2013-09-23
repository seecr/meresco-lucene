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

from seecr.test import SeecrTestCase
from meresco.lucene.composedquery import ComposedQuery

class ComposedQueryTest(SeecrTestCase):
    def testValidateComposedQuery(self):
        composedQuery = ComposedQuery()
        def assertValueError(message):
            try:
                composedQuery.validate()
                self.fail()
            except ValueError, e:
                self.assertEquals(message, str(e))
        assertValueError("Unsupported number of cores, expected exactly 2.")
        composedQuery.add(core='coreA', query=None)
        assertValueError("Unsupported number of cores, expected exactly 2.")
        composedQuery.add(core='coreB', query=None)
        assertValueError("Core for results not specified, use resultsFrom(core='core')")
        composedQuery.resultsFrom(core='coreC')
        assertValueError("Core in resultsFrom does not match the available cores, 'coreC' not in ['coreA', 'coreB']")
        composedQuery.resultsFrom(core='coreA')
        assertValueError("No match set for cores")
        composedQuery.addMatch(coreC='keyC', coreD='keyE')
        assertValueError("No match set for cores: ('coreA', 'coreB')")
        composedQuery.addMatch(coreA='keyA', coreB='keyB')
        composedQuery.validate()

    def testKeyNames(self):
        composedQuery = ComposedQuery()
        composedQuery.add(core='coreA', query=None)
        composedQuery.add(core='coreB', query=None)
        composedQuery.addMatch(coreA='keyA', coreB='keyB')
        self.assertEquals(('keyA', 'keyB'), composedQuery.keyNames('coreA', 'coreB'))
        self.assertEquals(('keyB', 'keyA'), composedQuery.keyNames('coreB', 'coreA'))

    def testUnite(self):
        mq = ComposedQuery()
        mq.add(core='coreA', query=None)
        mq.add(core='coreB', query=None)
        mq.addMatch(coreA='keyA', coreB='keyB')
        mq.unite(coreA='AQuery', coreB='anotherQuery')
        self.assertEquals(set([('coreA', 'keyA', 'AQuery'), ('coreB', 'keyB', 'anotherQuery')]), set(mq.unites()))

    def testFilterQueries(self):
        cq = ComposedQuery()
        cq.add(core='coreA', query='Q0', filterQueries=['Q1', 'Q2'], facets=['F0', 'F1'])
        cq.add(core='coreB', query='Q3', filterQueries=['Q4'])
        cq.addMatch(coreA='keyA', coreB='keyB')
        cq.unite(coreA='AQuery', coreB='anotherQuery')
        self.assertEquals(None, cq.stop)
        self.assertEquals(None, cq.start)
        self.assertEquals(None, cq.sortKeys)
        cq.stop = 10
        cq.start = 0
        cq.sortKeys = [dict(sortBy='field', sortDescending=True)]
        self.assertEquals('Q0', cq.queryFor('coreA'))
        self.assertEquals(['Q1', 'Q2'], cq.filterQueriesFor('coreA'))
        self.assertEquals(['F0', 'F1'], cq.facetsFor('coreA'))
        self.assertEquals(10, cq.stop)
        self.assertEquals(0, cq.start)
        self.assertEquals([dict(sortBy='field', sortDescending=True)], cq.sortKeys)

    def testAsDictFromDict(self):
        cq = ComposedQuery()
        cq.add(core='coreA', query='Q0', filterQueries=['Q1', 'Q2'], facets=['F0', 'F1'])
        cq.add(core='coreB', query='Q3', filterQueries=['Q4'])
        cq.addMatch(coreA='keyA', coreB='keyB')
        cq.unite(coreA='AQuery', coreB='anotherQuery')
        cq.start = 0
        cq.sortKeys = [dict(sortBy='field', sortDescending=True)]

        cq2 = ComposedQuery.fromDict(cq.asDict())
        self.assertEquals(0, cq2.start)
        self.assertEquals(None, cq2.stop)
        self.assertEquals(['Q0', 'Q1', 'Q2'], cq2.queriesFor('coreA'))
        self.assertEquals(('keyA', 'keyB'), cq2.keyNames('coreA', 'coreB'))
        self.assertEquals(('keyB', 'keyA'), cq2.keyNames('coreB', 'coreA'))

    def testConvertAllQueries(self):
        cq = ComposedQuery()
        cq.add(core='coreA', query='Q0', filterQueries=['Q1', 'Q2'])
        cq.add(core='coreB', query='Q3', filterQueries=['Q4'])
        cq.addMatch(coreA='keyA', coreB='keyB')
        cq.unite(coreA='Q5', coreB='Q6')
        cq.convertWith(lambda query: "Converted_%s" % query)

        self.assertEquals("Converted_Q0", cq.queryFor('coreA'))
        self.assertEquals(["Converted_Q1", "Converted_Q2"], cq.filterQueriesFor('coreA'))
        self.assertEquals("Converted_Q3", cq.queryFor('coreB'))
        self.assertEquals(["Converted_Q4"], cq.filterQueriesFor('coreB'))
        self.assertEquals(set([('coreA', 'keyA', 'Converted_Q5'), ('coreB', 'keyB', 'Converted_Q6')]), set(cq.unites()))

    def testSingleCoreQuery(self):
        cq = ComposedQuery()
        cq.add(core='coreA', query='Q0')
        cq.resultsFrom('coreA')
        cq.validate()


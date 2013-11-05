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
        composedQuery = ComposedQuery('coreA')
        composedQuery.setCoreQuery(core='coreA', query='Q0')
        composedQuery.setCoreQuery(core='coreB', query='Q1')
        self.assertValueError(composedQuery, "No match set for cores ('coreA', 'coreB')")
        composedQuery.addMatch(dict(core='coreC', uniqueKey='keyC'), dict(core='coreD', key='keyE'))
        self.assertValueError(composedQuery, 'Unsupported number of cores, expected at most 2.')

        composedQuery = ComposedQuery('coreA')        
        composedQuery.addMatch(dict(core='coreA', uniqueKey='keyA'), dict(core='coreB', key='keyB'))
        composedQuery.validate()
        self.assertEquals(2, composedQuery.numberOfCores)

    def testUniqueKeyDoesntMatchResultsFrom(self):
        composedQuery = ComposedQuery('coreA')
        composedQuery.addMatch(dict(core='coreA', key='keyA'), dict(core='coreB', key='ignored'))
        self.assertTrue(composedQuery.isSingleCoreQuery())
        self.assertValueError(composedQuery, "Match for result core 'coreA', for which one or more queries apply, must have a uniqueKey specification.")

        composedQuery.setCoreQuery('coreA', query='Q0')
        self.assertTrue(composedQuery.isSingleCoreQuery())
        self.assertValueError(composedQuery, "Match for result core 'coreA', for which one or more queries apply, must have a uniqueKey specification.")

        composedQuery.addMatch(dict(core='coreA', key='keyA'), dict(core='coreB', uniqueKey='keyB'))
        self.assertValueError(composedQuery, "Match for result core 'coreA', for which one or more queries apply, must have a uniqueKey specification.")
        composedQuery.addMatch(dict(core='coreA', uniqueKey='keyA'), dict(core='coreB', key='keyB'))
        composedQuery.validate()
        composedQuery.addMatch(dict(core='coreA', uniqueKey='keyA'), dict(core='coreB', uniqueKey='keyB'))
        composedQuery.validate()

    def testKeyNames(self):
        composedQuery = ComposedQuery('coreA')
        composedQuery.setCoreQuery(core='coreA', query=None)
        composedQuery.setCoreQuery(core='coreB', query=None)
        composedQuery.addMatch(dict(core='coreA', uniqueKey='keyA'), dict(core='coreB', key='keyB'))
        self.assertEquals(['keyA', 'keyB'], composedQuery.keyNames('coreA', 'coreB'))
        self.assertEquals(['keyB', 'keyA'], composedQuery.keyNames('coreB', 'coreA'))

    def testUnite(self):
        cq = ComposedQuery('coreA')
        cq.setCoreQuery(core='coreA', query=None)
        cq.setCoreQuery(core='coreB', query=None)
        cq.addMatch(dict(core='coreA', uniqueKey='keyA'), dict(core='coreB', key='keyB'))
        cq.unite(coreA='AQuery', coreB='anotherQuery')
        self.assertEquals(set([('coreA', 'keyA', 'AQuery'), ('coreB', 'keyB', 'anotherQuery')]), set(cq.unites()))

    def testFilterQueries(self):
        cq = ComposedQuery('coreA')
        cq.setCoreQuery(core='coreA', query='Q0', filterQueries=['Q1', 'Q2'], facets=['F0', 'F1'])
        cq.setCoreQuery(core='coreB', query='Q3', filterQueries=['Q4'])
        cq.addMatch(dict(core='coreA', uniqueKey='keyA'), dict(core='coreB', key='keyB'))
        cq.unite(coreA='AQuery', coreB='anotherQuery')
        self.assertEquals('Q0', cq.queryFor('coreA'))
        self.assertEquals(['Q1', 'Q2'], cq.filterQueriesFor('coreA'))
        self.assertEquals(['F0', 'F1'], cq.facetsFor('coreA'))

    def testPiggyBackOtherKwarsWithoutKnowningThemAllbyName(self):
        cq.ComposedQuery("core42", dict(some="other"))
        self.assertEquals({"some": "otherrrrr"}, cq.otherKwargs())

    def testAsDictFromDict(self):
        cq = ComposedQuery('coreA')
        cq.setCoreQuery(core='coreA', query='Q0', filterQueries=['Q1', 'Q2'], facets=['F0', 'F1'])
        cq.setCoreQuery(core='coreB', query='Q3', filterQueries=['Q4'])
        cq.addMatch(dict(core='coreA', uniqueKey='keyA'), dict(core='coreB', key='keyB'))
        cq.unite(coreA='AQuery', coreB='anotherQuery')
        cq.start = 0
        cq.sortKeys = [dict(sortBy='field', sortDescending=True)]

        d = cq.asDict()
        cq2 = ComposedQuery.fromDict(d)
        self.assertEquals('coreA', cq2._resultsFrom)
        self.assertEquals(0, cq2.start)
        self.assertEquals(None, cq2.stop)
        self.assertEquals(['Q0', 'Q1', 'Q2'], cq2.queriesFor('coreA'))
        self.assertEquals(['keyA', 'keyB'], cq2.keyNames('coreA', 'coreB'))
        self.assertEquals(['keyB', 'keyA'], cq2.keyNames('coreB', 'coreA'))

    def testAddFilterQueriesIncremental(self):
        cq = ComposedQuery('coreA')
        cq.addFilterQuery(core='coreA', query='Q1')
        cq.addFilterQuery(core='coreA', query='Q2')

        self.assertEquals(['Q1', 'Q2'], cq.filterQueriesFor('coreA'))

    def testAddFacetIncremental(self):
        cq = ComposedQuery('coreA')
        cq.addFacet(core='coreA', facet=dict(fieldname='Q1', maxTerms=10))
        cq.addFacet(core='coreA', facet=dict(fieldname='Q2', maxTerms=10))

        self.assertEquals([dict(fieldname='Q1', maxTerms=10), dict(fieldname='Q2', maxTerms=10)], cq.facetsFor('coreA'))

    def testConvertAllQueries(self):
        cq = ComposedQuery('coreA')
        cq.setCoreQuery(core='coreA', query='Q0', filterQueries=['Q1', 'Q2'])
        cq.setCoreQuery(core='coreB', query='Q3', filterQueries=['Q4'])
        cq.addMatch(dict(core='coreA', uniqueKey='keyA'), dict(core='coreB', key='keyB'))
        cq.unite(coreA='Q5', coreB='Q6')
        cq.convertWith(lambda query: "Converted_%s" % query)

        self.assertEquals("Converted_Q0", cq.queryFor('coreA'))
        self.assertEquals(["Converted_Q1", "Converted_Q2"], cq.filterQueriesFor('coreA'))
        self.assertEquals("Converted_Q3", cq.queryFor('coreB'))
        self.assertEquals(["Converted_Q4"], cq.filterQueriesFor('coreB'))
        self.assertEquals(set([('coreA', 'keyA', 'Converted_Q5'), ('coreB', 'keyB', 'Converted_Q6')]), set(cq.unites()))

    def testSingleCoreQuery(self):
        cq = ComposedQuery('coreA')
        cq.setCoreQuery(core='coreA', query='Q0')
        cq.validate()
        self.assertEquals(1, cq.numberOfCores)

    def testOneQueryInOtherCore(self):
        cq = ComposedQuery('coreA')
        cq.addMatch({'core': 'coreA', 'uniqueKey': 'keyA'}, {'core': 'coreB', 'key': 'keyB'})
        cq.setCoreQuery(core='coreB', query='Q0')
        cq.validate()
        self.assertEquals(None, cq.queryFor('coreA'))
        self.assertEquals('Q0', cq.queryFor('coreB'))

    def testUniteMakesItTwoCoreQuery(self):
        cq = ComposedQuery('coreA')
        cq.addMatch(dict(core='coreA', uniqueKey='keyA'), dict(core='coreB', key='keyB'))
        cq.setCoreQuery('coreA', query='A')
        cq.unite(coreA='Q5', coreB='Q6')
        cq.validate()
        self.assertEquals(('coreA', 'coreB'), cq.cores())

    def testIsSingleCoreQuery(self):
        cq = ComposedQuery('coreA')
        self.assertTrue(cq.isSingleCoreQuery())
        cq.addMatch(dict(core='coreA', uniqueKey='keyA'), dict(core='coreB', key='keyB'))
        self.assertTrue(cq.isSingleCoreQuery())
        cq.setCoreQuery('coreA', query='A')
        self.assertTrue(cq.isSingleCoreQuery())
        cq.setCoreQuery('coreB', query=None)
        self.assertTrue(cq.isSingleCoreQuery())        
        cq.unite(coreA='Q5', coreB='Q6')
        self.assertFalse(cq.isSingleCoreQuery())

        cq = ComposedQuery('coreA')
        cq.setCoreQuery('coreB', query='B')
        self.assertFalse(cq.isSingleCoreQuery())

        cq = ComposedQuery('coreA')
        cq.setCoreQuery('coreB', query=None, filterQueries=['B'])
        self.assertFalse(cq.isSingleCoreQuery())

    def assertValueError(self, composedQuery, message):
        try:
            composedQuery.validate()
            self.fail("should have raised ValueError")
        except ValueError, e:
            self.assertEquals(message, str(e))


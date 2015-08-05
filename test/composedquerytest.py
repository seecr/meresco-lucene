## begin license ##
#
# "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
#
# Copyright (C) 2013-2015 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2013-2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
# Copyright (C) 2015 Koninklijke Bibliotheek (KB) http://www.kb.nl
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
        self.assertValidateRaisesValueError(composedQuery, "No match set for cores ('coreA', 'coreB')")

        composedQuery = ComposedQuery('coreA', query="A")
        composedQuery.addMatch(dict(core='coreA', uniqueKey='keyA'), dict(core='coreB', key='keyB'))
        composedQuery.validate()
        self.assertEquals(1, composedQuery.numberOfUsedCores)

    def testValidateComposedQueryForThreeCores(self):
        composedQuery = ComposedQuery('coreA')
        composedQuery.setCoreQuery(core='coreA', query='Q0')
        composedQuery.setCoreQuery(core='coreB', query='Q1')
        composedQuery.setCoreQuery(core='coreC', query='Q2')
        self.assertValidateRaisesValueError(composedQuery, "No match set for cores ('coreA', 'coreB')")

        composedQuery.addMatch(dict(core='coreA', uniqueKey='keyA'), dict(core='coreB', key='keyB'))
        self.assertValidateRaisesValueError(composedQuery, "No match set for cores ('coreA', 'coreC')")

        composedQuery.addMatch(dict(core='coreA', uniqueKey='keyA'), dict(core='coreC', key='keyC'))
        composedQuery.validate()
        self.assertEquals(3, composedQuery.numberOfUsedCores)

    def testSameCoreInDifferentMatchesRequiredToHaveSameKeyForNow(self):
        composedQuery = ComposedQuery('coreA', query='qA')
        composedQuery.setCoreQuery('coreB', query='qB')
        composedQuery.setCoreQuery('coreC', query='qC')
        composedQuery.addMatch(dict(core='coreA', uniqueKey='keyA'), dict(core='coreB', key='keyB'))
        self.assertRaises(ValueError, lambda: composedQuery.addMatch(dict(core='coreA', key='keyX'), dict(core='coreC', key='keyC')))

    def testAtMostOneMultiCoreOr(self):
        composedQuery = ComposedQuery('coreA')
        composedQuery.addMatch(dict(core='coreA', uniqueKey='keyA'), dict(core='coreB', key='keyB'))
        composedQuery.addMatch(dict(core='coreA', uniqueKey='keyA'), dict(core='coreC', key='keyC'))
        composedQuery.addUnite(dict(core='coreA', query='qA'), dict(core='coreB', query='qB'))
        self.assertRaises(ValueError, lambda: composedQuery.addUnite(dict(core='coreA', query='qA'), dict('coreC', query='qC')))

    def testUniqueKeyDoesntMatchResultsFrom(self):
        composedQuery = ComposedQuery('coreA', query='A').setCoreQuery('coreB', query='bQ')
        self.assertRaises(ValueError, lambda: composedQuery.addMatch(dict(core='coreA', key='keyA'), dict(core='coreB', key='keyB')))
        self.assertRaises(ValueError, lambda: composedQuery.addMatch(dict(core='coreA', key='keyA'), dict(core='coreB', uniqueKey='keyB')))
        composedQuery.addMatch(dict(core='coreA', uniqueKey='keyA'), dict(core='coreB', key='keyB'))
        composedQuery.validate()
        composedQuery.addMatch(dict(core='coreA', uniqueKey='keyA'), dict(core='coreB', uniqueKey='keyB'))
        composedQuery.validate()

    def testMatchesMustAlwaysIncludeResultsFrom(self):
        composedQuery = ComposedQuery('coreA', query='qA')
        composedQuery.setCoreQuery('coreB', query='qB')
        composedQuery.setCoreQuery('coreC', query='qC')
        self.assertRaises(ValueError, lambda: composedQuery.addMatch(dict(core='coreB', key='keyB'), dict(core='coreC', key='keyC')))

    def testKeyName(self):
        cq = ComposedQuery('coreA')
        cq.addMatch(dict(core='coreA', uniqueKey='keyA'), dict(core='coreB', key='keyB'))
        cq.addFacet(core='coreB', facet='F0')
        self.assertEquals('keyA', cq.keyName('coreA', 'coreB'))
        self.assertEquals('keyB', cq.keyName('coreB', 'coreA'))
        self.assertEquals(set(['keyA']), cq.keyNames('coreA'))

    def testKeyNamesDifferPerCore(self):
        cq = ComposedQuery('coreA')
        cq.addMatch(dict(core='coreA', uniqueKey='keyA'), dict(core='coreB', key='keyB'))
        cq.addMatch(dict(core='coreA', uniqueKey='keyAC'), dict(core='coreC', key='keyC'))
        cq.addFacet(core='coreB', facet='F0')
        cq.addFacet(core='coreC', facet='F1')
        self.assertEquals('keyAC', cq.keyName('coreA', 'coreC'))
        self.assertEquals('keyC', cq.keyName('coreC', 'coreA'))
        self.assertEquals(set(['keyA', 'keyAC']), cq.keyNames('coreA'))

    def testUnite(self):
        cq = ComposedQuery('coreA')
        cq.setCoreQuery(core='coreA', query=None)
        cq.setCoreQuery(core='coreB', query=None)
        cq.addMatch(dict(core='coreA', uniqueKey='keyA'), dict(core='coreB', key='keyB'))
        cq.addUnite(dict(core='coreA', query='AQuery'), dict(core='coreB', query='anotherQuery'))
        self.assertEqual(1, len(cq.unites))
        queries = list(cq.unites[0].queries())
        self.assertEquals(({'query': 'AQuery', 'keyName': 'keyA', 'core': 'coreA'}, 'keyA'), queries[0])
        self.assertEquals(({'query': 'anotherQuery', 'keyName': 'keyB', 'core': 'coreB'}, 'keyA'), queries[1])

    def testFilterQueries(self):
        cq = ComposedQuery('coreA')
        cq.setCoreQuery(core='coreA', query='Q0')
        cq.addFilterQuery(core='coreA', query='Q1')
        cq.addFilterQuery(core='coreA', query='Q2')
        cq.addFacet(core='coreA', facet='F0')
        cq.addFacet(core='coreA', facet='F1')
        cq.setCoreQuery(core='coreB', query='Q3')
        cq.addMatch(dict(core='coreA', uniqueKey='keyA'), dict(core='coreB', key='keyB'))
        cq.addUnite(dict(core='coreA', query='AQuery'), dict(core='coreB', query='anotherQuery'))
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
        cq = ComposedQuery('coreA')
        cq.setCoreQuery(core='coreA', query='Q0')
        cq.addFilterQuery(core='coreA', query='Q1')
        cq.addFilterQuery(core='coreA', query='Q2')
        cq.addFacet(core='coreA', facet='F0')
        cq.addFacet(core='coreA', facet='F1')
        cq.setCoreQuery(core='coreB', query='Q3')
        cq.addMatch(dict(core='coreA', uniqueKey='keyA'), dict(core='coreB', key='keyB'))
        cq.addUnite(dict(core='coreA', query='AQuery'), dict(core='coreB', query='anotherQuery'))
        cq.start = 0
        cq.sortKeys = [dict(sortBy='field', sortDescending=True)]

        d = cq.asDict()
        cq2 = ComposedQuery.fromDict(d)
        self.assertEquals('coreA', cq2.resultsFrom)
        self.assertEquals(0, cq2.start)
        self.assertEquals(None, cq2.stop)
        self.assertEquals(['Q0', 'Q1', 'Q2'], cq2.queriesFor('coreA'))
        self.assertEquals(['F0', 'F1'], cq2.facetsFor('coreA'))
        self.assertEquals('keyA', cq2.keyName('coreA', 'coreB'))
        self.assertEquals('keyB', cq2.keyName('coreB', 'coreA'))
        self.assertEqual(1, len(cq2.unites))
        queries = list(cq2.unites[0].queries())
        self.assertEquals(({'core': 'coreA', 'keyName': 'keyA', 'query': 'AQuery'}, 'keyA'), queries[0])
        self.assertEquals(({'core': 'coreB', 'keyName': 'keyB', 'query': 'anotherQuery'}, 'keyA'), queries[1])

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
        cq.setCoreQuery(core='coreA', query='Q0')
        cq.addFilterQuery('coreA', 'Q1')
        cq.addFilterQuery('coreA', 'Q2')
        cq.setCoreQuery(core='coreB', query='Q3')
        cq.addFilterQuery('coreB', 'Q4')
        cq.addMatch(dict(core='coreA', uniqueKey='keyA'), dict(core='coreB', key='keyB'))
        cq.addUnite(dict(core='coreA', query='Q5'), dict(core='coreB', query='Q6'))
        convertCoreA = lambda query: "Converted_A_{0}".format(query)
        convertCoreB = lambda query: "Converted_B_{0}".format(query)
        cq.convertWith(coreA=convertCoreA, coreB=convertCoreB)

        self.assertEquals("Converted_A_Q0", cq.queryFor('coreA'))
        self.assertEquals(["Converted_A_Q1", "Converted_A_Q2"], cq.filterQueriesFor('coreA'))
        self.assertEquals("Converted_B_Q3", cq.queryFor('coreB'))
        self.assertEquals(["Converted_B_Q4"], cq.filterQueriesFor('coreB'))
        self.assertEqual(1, len(cq.unites))
        queries = list(cq.unites[0].queries())
        self.assertEquals('Converted_A_Q5', queries[0][0]['query'])
        self.assertEquals('Converted_B_Q6', queries[1][0]['query'])

    def testSingleCoreQuery(self):
        cq = ComposedQuery('coreA')
        cq.setCoreQuery(core='coreA', query='Q0')
        cq.validate()
        self.assertEquals(1, cq.numberOfUsedCores)

    def testUniteMakesItTwoCoreQuery(self):
        cq = ComposedQuery('coreA')
        cq.addMatch(dict(core='coreA', uniqueKey='keyA'), dict(core='coreB', key='keyB'))
        cq.setCoreQuery('coreA', query='A')
        cq.addUnite(dict(core='coreA', query='Q5'), dict(core='coreB', query='Q6'))
        cq.validate()
        self.assertEquals(set(['coreA', 'coreB']), cq.cores)

    def testIsSingleCoreQuery(self):
        cq = ComposedQuery('coreA')
        cq.addMatch(dict(core='coreA', uniqueKey='keyA'), dict(core='coreB', key='keyB'))
        cq.setCoreQuery('coreA', query='A')
        self.assertTrue(cq.isSingleCoreQuery())
        cq.addUnite(dict(core='coreA', query='Q5'), dict(core='coreB', query='Q6'))
        self.assertFalse(cq.isSingleCoreQuery())

    def testAddRankQuery(self):
        cq = ComposedQuery('coreA')
        cq.setRankQuery('coreB', 'qB')
        self.assertValidateRaisesValueError(cq, "No match set for cores ('coreA', 'coreB')")
        cq.addMatch(dict(core='coreA', uniqueKey='kA'), dict(core='coreB', key='kB'))
        self.assertEquals('qB', cq.rankQueryFor('coreB'))
        cq.convertWith(coreB=lambda q: "converted_" + q)
        self.assertEquals('converted_qB', cq.rankQueryFor('coreB'))

    def testAddDrilldownQuery(self):
        cq = ComposedQuery('coreA')
        cq.addDrilldownQuery('coreB', ('field', ['value']))
        self.assertValidateRaisesValueError(cq, "No match set for cores ('coreA', 'coreB')")
        cq.addMatch(dict(core='coreA', uniqueKey='kA'), dict(core='coreB', key='kB'))
        self.assertEquals([('field', ['value'])], cq.drilldownQueriesFor('coreB'))
        cq.convertWith(coreB=lambda q: "converted_" + q)
        self.assertEquals([('field', ['value'])], cq.drilldownQueriesFor('coreB'))

    def testAddOtherCoreFacetFilter(self):
        cq = ComposedQuery('coreA')
        cq.addOtherCoreFacetFilter('coreB', 'field=value')
        self.assertValidateRaisesValueError(cq, "No match set for cores ('coreA', 'coreB')")
        cq.addMatch(dict(core='coreA', uniqueKey='kA'), dict(core='coreB', key='kB'))
        self.assertEquals(['field=value'], cq.otherCoreFacetFiltersFor('coreB'))
        cq.convertWith(coreB=lambda q: "converted_" + q)
        self.assertEquals(['converted_field=value'], cq.otherCoreFacetFiltersFor('coreB'))

    def testRepr(self):
        class AQuery(object):
            def __repr__(self):
                return 'NOT USED'
            def __str__(self):
                return 'AQuery'
        cq = ComposedQuery('coreA')
        cq.setCoreQuery(core='coreA', query='Q0')
        cq.addFilterQuery(core='coreA', query='Q1')
        cq.addFilterQuery(core='coreA', query='Q2')
        cq.addFacet(core='coreA', facet='F0')
        cq.addFacet(core='coreA', facet='F1')
        cq.setCoreQuery(core='coreB', query='Q3')
        cq.addMatch(dict(core='coreA', uniqueKey='keyA'), dict(core='coreB', key='keyB'))
        cq.addUnite(dict(core='coreA', query=AQuery()), dict(core='coreB', query='anotherQuery'))
        cq.start = 0
        cq.sortKeys = [dict(sortBy='field', sortDescending=True)]
        cq.storedFields = ['stored_field']

        self.assertEquals({
            'type': 'ComposedQuery',
            'query': {
                "cores": ["coreB", "coreA"],
                "drilldownQueries": {},
                "facets": {"coreA": ["F0", "F1"]},
                "filterQueries": {"coreA": ["Q1", "Q2"]},
                "matches": {"coreA->coreB": [{"core": "coreA", "uniqueKey": "keyA"}, {"core": "coreB", "key": "keyB"}]},
                "otherCoreFacetFilters": {},
                "queries": {"coreA": "Q0", "coreB": "Q3"},
                "rankQueries": {},
                "resultsFrom": "coreA",
                "sortKeys": [{"sortBy": "field", "sortDescending": True}],
                "start": 0,
                "storedFields": ['stored_field'],
                "unites": [{"A": ["coreA", "AQuery"], "B": ["coreB", "anotherQuery"]}]
            }
        }, cq.infoDict())

    def assertValidateRaisesValueError(self, composedQuery, message):
        try:
            composedQuery.validate()
            self.fail("should have raised ValueError")
        except ValueError, e:
            self.assertEquals(message, str(e))

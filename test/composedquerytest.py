## begin license ##
#
# "Meresco Lucene" is a set of components and tools to integrate Lucene into Meresco
#
# Copyright (C) 2013-2016, 2020-2021 Seecr (Seek You Too B.V.) https://seecr.nl
# Copyright (C) 2013-2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
# Copyright (C) 2015-2016 Koninklijke Bibliotheek (KB) http://www.kb.nl
# Copyright (C) 2016, 2020-2021 Stichting Kennisnet https://www.kennisnet.nl
# Copyright (C) 2021 Data Archiving and Network Services https://dans.knaw.nl
# Copyright (C) 2021 SURF https://www.surf.nl
# Copyright (C) 2021 The Netherlands Institute for Sound and Vision https://beeldengeluid.nl
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
        self.assertEqual(1, composedQuery.numberOfUsedCores)

    def testValidateComposedQueryForThreeCores(self):
        composedQuery = ComposedQuery('coreA')
        composedQuery.setCoreQuery(core='coreA', query='Q0')
        composedQuery.setCoreQuery(core='coreB', query='Q1')
        self.assertValidateRaisesValueError(composedQuery, "No match set for cores ('coreA', 'coreB')")
        composedQuery.setCoreQuery(core='coreC', query='Q2')

        composedQuery.addMatch(dict(core='coreA', uniqueKey='keyA'), dict(core='coreB', key='keyB'))
        self.assertValidateRaisesValueError(composedQuery, "No match set for cores ('coreA', 'coreC')")

        composedQuery.addMatch(dict(core='coreA', uniqueKey='keyA'), dict(core='coreC', key='keyC'))
        composedQuery.validate()
        self.assertEqual(3, composedQuery.numberOfUsedCores)

    def testValidateComposedQueryForInvalidJson(self):
        composedQuery = ComposedQuery('coreA', query='Q0')
        composedQuery.relationalFilterJson = 'not JSON'
        self.assertValidateRaisesValueError(composedQuery, "Value 'not JSON' for 'relationalFilterJson' can not be parsed as JSON.")
        composedQuery.relationalFilterJson = '{"type": "MockJoinQuery"}'
        composedQuery.validate()

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
        self.assertEqual('keyA', cq.keyName('coreA', 'coreB'))
        self.assertEqual('keyB', cq.keyName('coreB', 'coreA'))
        self.assertEqual(set(['keyA']), cq.keyNames('coreA'))

    def testKeyNamesDifferPerCore(self):
        cq = ComposedQuery('coreA')
        cq.addMatch(dict(core='coreA', uniqueKey='keyA'), dict(core='coreB', key='keyB'))
        cq.addMatch(dict(core='coreA', uniqueKey='keyAC'), dict(core='coreC', key='keyC'))
        cq.addFacet(core='coreB', facet='F0')
        cq.addFacet(core='coreC', facet='F1')
        self.assertEqual('keyAC', cq.keyName('coreA', 'coreC'))
        self.assertEqual('keyC', cq.keyName('coreC', 'coreA'))
        self.assertEqual(set(['keyA', 'keyAC']), cq.keyNames('coreA'))

    def testUnite(self):
        cq = ComposedQuery('coreA')
        cq.setCoreQuery(core='coreA', query=None)
        cq.setCoreQuery(core='coreB', query=None)
        cq.addMatch(dict(core='coreA', uniqueKey='keyA'), dict(core='coreB', key='keyB'))
        cq.addUnite(dict(core='coreA', query='AQuery'), dict(core='coreB', query='anotherQuery'))
        self.assertEqual(1, len(cq.unites))
        queries = list(cq.unites[0].queries())
        self.assertEqual(({'query': 'AQuery', 'keyName': 'keyA', 'core': 'coreA'}, 'keyA'), queries[0])
        self.assertEqual(({'query': 'anotherQuery', 'keyName': 'keyB', 'core': 'coreB'}, 'keyA'), queries[1])

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
        self.assertEqual(None, cq.stop)
        self.assertEqual(None, cq.start)
        self.assertEqual([], cq.sortKeys)
        cq.stop = 10
        cq.start = 0
        cq.sortKeys = [dict(sortBy='field', sortDescending=True)]
        self.assertEqual('Q0', cq.queryFor('coreA'))
        self.assertEqual(['Q1', 'Q2'], cq.filterQueriesFor('coreA'))
        self.assertEqual(['F0', 'F1'], cq.facetsFor('coreA'))
        self.assertEqual(10, cq.stop)
        self.assertEqual(0, cq.start)
        self.assertEqual([dict(sortBy='field', sortDescending=True)], cq.sortKeys)

    def testAsDictFromDict(self):
        cq = ComposedQuery('coreA')
        cq.setCoreQuery(core='coreA', query='Q0')
        cq.addFilterQuery(core='coreA', query='Q1')
        cq.addFilterQuery(core='coreA', query='Q2')
        cq.relationalFilter = '{"type": "madeUpJoinQuery"}'
        cq.addFacet(core='coreA', facet='F0')
        cq.addFacet(core='coreA', facet='F1')
        cq.setCoreQuery(core='coreB', query='Q3')
        cq.addMatch(dict(core='coreA', uniqueKey='keyA'), dict(core='coreB', key='keyB'))
        cq.addUnite(dict(core='coreA', query='AQuery'), dict(core='coreB', query='anotherQuery'))
        cq.start = 0
        cq.sortKeys = [dict(sortBy='field', sortDescending=True)]
        cq.clustering = True
        cq.clusteringConfig = {'clusteringEps': 0.2}
        cq.rankQueryScoreRatio = 0.75

        d = cq.asDict()
        cq2 = ComposedQuery.fromDict(d)
        self.assertEqual('coreA', cq2.resultsFrom)
        self.assertEqual(0, cq2.start)
        self.assertEqual(None, cq2.stop)
        self.assertEqual(['Q0', 'Q1', 'Q2'], cq2.queriesFor('coreA'))
        self.assertEqual('{"type": "madeUpJoinQuery"}', cq2.relationalFilter)
        self.assertEqual(['F0', 'F1'], cq2.facetsFor('coreA'))
        self.assertEqual('keyA', cq2.keyName('coreA', 'coreB'))
        self.assertEqual('keyB', cq2.keyName('coreB', 'coreA'))
        self.assertEqual(1, len(cq2.unites))
        queries = list(cq2.unites[0].queries())
        self.assertEqual(({'core': 'coreA', 'keyName': 'keyA', 'query': 'AQuery'}, 'keyA'), queries[0])
        self.assertEqual(({'core': 'coreB', 'keyName': 'keyB', 'query': 'anotherQuery'}, 'keyA'), queries[1])
        self.assertEqual({'clusteringEps': 0.2}, cq2.clusteringConfig)
        self.assertEqual(0.75, cq2.rankQueryScoreRatio)

    def testAddFilterQueriesIncremental(self):
        cq = ComposedQuery('coreA')
        cq.addFilterQuery(core='coreA', query='Q1')
        cq.addFilterQuery(core='coreA', query='Q2')

        self.assertEqual(['Q1', 'Q2'], cq.filterQueriesFor('coreA'))

    def testAddFacetIncremental(self):
        cq = ComposedQuery('coreA')
        cq.addFacet(core='coreA', facet=dict(fieldname='Q1', maxTerms=10))
        cq.addFacet(core='coreA', facet=dict(fieldname='Q2', maxTerms=10))

        self.assertEqual([dict(fieldname='Q1', maxTerms=10), dict(fieldname='Q2', maxTerms=10)], cq.facetsFor('coreA'))

    def testConvertAllQueries(self):
        cq = ComposedQuery('coreA')
        cq.setCoreQuery(core='coreA', query='Q0')
        cq.addFilterQuery('coreA', 'Q1')
        cq.addFilterQuery('coreA', 'Q2')
        cq.setCoreQuery(core='coreB', query='Q3')
        cq.addFilterQuery('coreB', 'Q4')
        cq.addMatch(dict(core='coreA', uniqueKey='keyA'), dict(core='coreB', key='keyB'))
        cq.addUnite(dict(core='coreA', query='Q5'), dict(core='coreB', query='Q6'))
        convertCoreA = lambda query, **kwargs: "Converted_A_{0}".format(query)
        convertCoreB = lambda query, **kwargs: "Converted_B_{0}".format(query)
        cq.convertWith(coreA=convertCoreA, coreB=convertCoreB)

        self.assertEqual("Converted_A_Q0", cq.queryFor('coreA'))
        self.assertEqual(["Converted_A_Q1", "Converted_A_Q2"], cq.filterQueriesFor('coreA'))
        self.assertEqual("Converted_B_Q3", cq.queryFor('coreB'))
        self.assertEqual(["Converted_B_Q4"], cq.filterQueriesFor('coreB'))
        self.assertEqual(1, len(cq.unites))
        queries = list(cq.unites[0].queries())
        self.assertEqual('Converted_A_Q5', queries[0][0]['query'])
        self.assertEqual('Converted_B_Q6', queries[1][0]['query'])

    def testConvertAllQueriesWithUnqualifiedTermFields(self):
        cq = ComposedQuery('coreA')
        cq.setCoreQuery(core='coreA', query='Q0')
        cq.addFilterQuery('coreA', 'Q1')
        cq.addFilterQuery('coreA', 'Q2')
        cq.setCoreQuery(core='coreB', query='Q3')
        cq.addFilterQuery('coreB', 'Q4')
        cq.addMatch(dict(core='coreA', uniqueKey='keyA'), dict(core='coreB', key='keyB'))
        cq.addUnite(dict(core='coreA', query='Q5'), dict(core='coreB', query='Q6'))
        cq.unqualifiedTermFields = [('field0', 2.0), ('field1', 3.0)]

        convertCoreA = lambda query, unqualifiedTermFields=None, **kwargs: "Converted_A_{0}_{1}".format(query, not unqualifiedTermFields is None)
        convertCoreB = lambda query, **kwargs: "Converted_B_{0}".format(query)
        cq.convertWith(coreA=convertCoreA, coreB=convertCoreB)

        self.assertEqual("Converted_A_Q0_True", cq.queryFor('coreA'))
        self.assertEqual(["Converted_A_Q1_True", "Converted_A_Q2_True"], cq.filterQueriesFor('coreA'))
        self.assertEqual("Converted_B_Q3", cq.queryFor('coreB'))
        self.assertEqual(["Converted_B_Q4"], cq.filterQueriesFor('coreB'))
        self.assertEqual(1, len(cq.unites))
        uniteQueries = list(cq.unites[0].queries())
        self.assertEqual('Converted_A_Q5_True', uniteQueries[0][0]['query'])
        self.assertEqual('Converted_B_Q6', uniteQueries[1][0]['query'])

    def testSingleCoreQuery(self):
        cq = ComposedQuery('coreA')
        cq.setCoreQuery(core='coreA', query='Q0')
        cq.validate()
        self.assertEqual(1, cq.numberOfUsedCores)

    def testUniteMakesItTwoCoreQuery(self):
        cq = ComposedQuery('coreA')
        cq.addMatch(dict(core='coreA', uniqueKey='keyA'), dict(core='coreB', key='keyB'))
        cq.setCoreQuery('coreA', query='A')
        cq.addUnite(dict(core='coreA', query='Q5'), dict(core='coreB', query='Q6'))
        cq.validate()
        self.assertEqual(set(['coreA', 'coreB']), cq.cores)

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
        self.assertEqual('qB', cq.rankQueryFor('coreB'))
        cq.convertWith(coreB=lambda q: "converted_" + q)
        self.assertEqual('converted_qB', cq.rankQueryFor('coreB'))

    def testAddDrilldownQuery(self):
        cq = ComposedQuery('coreA')
        cq.addDrilldownQuery('coreB', ('field', ['value']))
        self.assertValidateRaisesValueError(cq, "No match set for cores ('coreA', 'coreB')")
        cq.addMatch(dict(core='coreA', uniqueKey='kA'), dict(core='coreB', key='kB'))
        self.assertEqual([('field', ['value'])], cq.drilldownQueriesFor('coreB'))
        cq.convertWith(coreB=lambda q: "converted_" + q)
        self.assertEqual([('field', ['value'])], cq.drilldownQueriesFor('coreB'))

    def testAddOtherCoreFacetFilter(self):
        cq = ComposedQuery('coreA')
        cq.addOtherCoreFacetFilter('coreB', 'field=value')
        self.assertValidateRaisesValueError(cq, "No match set for cores ('coreA', 'coreB')")
        cq.addMatch(dict(core='coreA', uniqueKey='kA'), dict(core='coreB', key='kB'))
        self.assertEqual(['field=value'], cq.otherCoreFacetFiltersFor('coreB'))
        cq.convertWith(coreB=lambda q: "converted_" + q)
        self.assertEqual(['converted_field=value'], cq.otherCoreFacetFiltersFor('coreB'))

    def testAddFilterQueryAfterConversion(self):
        cq = ComposedQuery('coreA')
        cq.setCoreQuery('coreA', query='A')
        cq.convertWith(coreA=lambda q, **kwargs: "converted_" + q)
        self.assertEqual('converted_A', cq.queryFor('coreA'))
        # Assert the following does not raise KeyError
        cq.addFilterQuery('coreA', 'field=value')
        cq.addFacet('coreA', 'F0')
        cq.addDrilldownQuery('coreA', 'drilldownQuery')
        cq.addOtherCoreFacetFilter('coreA', 'q')

    def testExcludeFilter(self):
        cq = ComposedQuery('coreA')
        cq.addExcludeFilterQuery('coreA', 'excludeMe')
        self.assertEqual(["excludeMe"], cq.excludeFilterQueriesFor('coreA'))
        cq2 = ComposedQuery.fromDict(cq.asDict())
        self.assertEqual(["excludeMe"], cq2.excludeFilterQueriesFor('coreA'))

        cq.convertWith(coreA=lambda q, **kwargs: "converted_" + q)
        self.assertEqual(["converted_excludeMe"], cq.excludeFilterQueriesFor('coreA'))
        self.assertEqual({"coreA":["converted_excludeMe"]}, cq.asDict()['_excludeFilterQueries'])

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
        cq.relationalFilter = '{"type": "MadeUpJoinQuery"}'
        cq.addFacet(core='coreA', facet='F0')
        cq.addFacet(core='coreA', facet='F1')
        cq.setCoreQuery(core='coreB', query='Q3')
        cq.addMatch(dict(core='coreA', uniqueKey='keyA'), dict(core='coreB', key='keyB'))
        cq.addUnite(dict(core='coreA', query=AQuery()), dict(core='coreB', query='anotherQuery'))
        cq.start = 0
        cq.sortKeys = [dict(sortBy='field', sortDescending=True)]
        cq.storedFields = ['stored_field']

        self.assertEqual({
            'type': 'ComposedQuery',
            'query': {
                "cores": ["coreA", "coreB"],
                "drilldownQueries": {},
                "facets": {"coreA": ["F0", "F1"]},
                "filterQueries": {"coreA": ["Q1", "Q2"]},
                "excludeFilterQueries": {},
                "matches": {"coreA->coreB": [{"core": "coreA", "uniqueKey": "keyA"}, {"core": "coreB", "key": "keyB"}]},
                "otherCoreFacetFilters": {},
                "queries": {"coreA": "Q0", "coreB": "Q3"},
                "rankQueries": {},
                'relationalFilter': '{"type": "MadeUpJoinQuery"}',
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
        except ValueError as e:
            self.assertEqual(message, str(e))

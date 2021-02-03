## begin license ##
#
# "Meresco Lucene" is a set of components and tools to integrate Lucene into Meresco
#
# Copyright (C) 2013-2016, 2019-2021 Seecr (Seek You Too B.V.) https://seecr.nl
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

from seecr.test import SeecrTestCase, CallTrace

from cqlparser import parseString as parseCQL, cqlToExpression
from cqlparser.cqltoexpression import QueryExpression

from weightless.core import be, consume, retval
from meresco.core import Observable

from meresco.lucene import LuceneResponse
from meresco.lucene.converttocomposedquery import ConvertToComposedQuery


class ConvertToComposedQueryTest(SeecrTestCase):
    def setUp(self):
        SeecrTestCase.setUp(self)
        self.setupDna()

    def setupDna(self, dedupFieldName="__key__.dedup", dedupSortFieldName="__key__.date", dedupByDefault=True):
        self.observer = CallTrace('observer', emptyGeneratorMethods=['executeComposedQuery'])
        self.response = LuceneResponse()
        def executeComposedQuery(*args, **kwargs):
            raise StopIteration(self.response)
            yield
        self.observer.methods['executeComposedQuery'] = executeComposedQuery
        self.tree = be(
            (Observable(),
                (ConvertToComposedQuery(
                        resultsFrom='defaultCore',
                        matches=[(
                                dict(core='defaultCore', uniqueKey='keyDefault'),
                                dict(core='otherCore', key='keyOther')
                            ),
                            (
                                dict(core='defaultCore', uniqueKey='key1'),
                                dict(core='aDifferentKore', key='key2')
                            )
                        ],
                        dedupFieldName=dedupFieldName,
                        dedupSortFieldName=dedupSortFieldName,
                        dedupByDefault=dedupByDefault,
                        drilldownFieldnamesTranslate=lambda name: 'prefix.' + name if name == 'toBePrefixed' else name,
                    ),
                    (self.observer,)
                )
            )
        )

    def testXFilter(self):
        consume(self.tree.any.executeQuery(cqlAbstractSyntaxTree=parseCQL('*'), extraArguments={'x-filter': ['otherCore.prefix:field=value']}, facets=[], start=1))
        self.assertEqual(['executeComposedQuery'], self.observer.calledMethodNames())
        cq = self.observer.calledMethods[0].kwargs['query']
        cq.validate()
        self.assertEqual(1, cq.start)
        self.assertEqual(set(['defaultCore', 'otherCore']), cq.cores)
        self.assertEqual('keyDefault', cq.keyName('defaultCore', 'otherCore'))
        self.assertEqual('keyOther', cq.keyName('otherCore', 'defaultCore'))
        self.assertEqual([cqlToExpression("prefix:field=value")], cq.queriesFor('otherCore'))
        self.assertEqual([cqlToExpression('*')], cq.queriesFor('defaultCore'))

    def testFilterQuery(self):
        consume(self.tree.any.executeQuery(cqlAbstractSyntaxTree=parseCQL('*'), filterQueries=[('otherCore', 'prefix:field=value')], facets=[], start=1))
        self.assertEqual(['executeComposedQuery'], self.observer.calledMethodNames())
        cq = self.observer.calledMethods[0].kwargs['query']
        cq.validate()
        self.assertEqual(1, cq.start)
        self.assertEqual(set(['defaultCore', 'otherCore']), cq.cores)
        self.assertEqual('keyDefault', cq.keyName('defaultCore', 'otherCore'))
        self.assertEqual('keyOther', cq.keyName('otherCore', 'defaultCore'))
        self.assertEqual([cqlToExpression("prefix:field=value")], cq.queriesFor('otherCore'))
        self.assertEqual([cqlToExpression('*')], cq.queriesFor('defaultCore'))

    def testExcludeFilterQuery(self):
        consume(self.tree.any.executeQuery(query=cqlToExpression('*'), excludeFilterQueries=[('otherCore', 'prefix:field=value')], facets=[], start=1))
        self.assertEqual(['executeComposedQuery'], self.observer.calledMethodNames())
        cq = self.observer.calledMethods[0].kwargs['query']
        cq.validate()
        self.assertEqual([cqlToExpression("prefix:field=value")], cq.excludeFilterQueriesFor('otherCore'))


    def testMatchesOptional(self):
        self.tree = be(
            (Observable(),
                (ConvertToComposedQuery(resultsFrom='defaultCore'),
                    (self.observer,)
                )
            )
        )
        consume(self.tree.any.executeQuery(cqlAbstractSyntaxTree=parseCQL('*'), extraArguments={'x-filter': ['prefix:field=value']}, facets=[], start=1))
        self.assertEqual(['executeComposedQuery'], self.observer.calledMethodNames())
        cq = self.observer.calledMethods[0].kwargs['query']
        cq.validate()
        self.assertEqual(1, cq.start)
        self.assertEqual(set(['defaultCore']), cq.cores)
        self.assertRaises(KeyError, lambda: cq.keyName('defaultCore', 'otherCore'))
        self.assertEqual([cqlToExpression("*"), cqlToExpression("prefix:field=value")], cq.queriesFor('defaultCore'))

    def testTwoXFiltersForSameCore(self):
        consume(self.tree.any.executeQuery(cqlAbstractSyntaxTree=parseCQL('*'), extraArguments={'x-filter': ['otherCore.prefix:field=value', 'otherCore.field2=value2']}, facets=[]))
        self.assertEqual(['executeComposedQuery'], self.observer.calledMethodNames())
        cq = self.observer.calledMethods[0].kwargs['query']
        cq.validate()
        self.assertEqual('defaultCore', cq.resultsFrom)
        self.assertEqual(set(['defaultCore', 'otherCore']), cq.cores)
        self.assertEqual('keyDefault', cq.keyName('defaultCore', 'otherCore'))
        self.assertEqual('keyOther', cq.keyName('otherCore', 'defaultCore'))
        self.assertEqual([cqlToExpression("prefix:field=value"), cqlToExpression('field2=value2')], cq.queriesFor('otherCore'))

    def testXFilterWhichIsNoJoinQuery(self):
        consume(self.tree.any.executeQuery(cqlAbstractSyntaxTree=parseCQL('*'), extraArguments={'x-filter': ['prefix:field=value']}, facets=[], someKwarg='someValue'))
        self.assertEqual(['executeComposedQuery'], self.observer.calledMethodNames())
        cq = self.observer.calledMethods[0].kwargs['query']
        cq.validate()
        self.assertEqual('defaultCore', cq.resultsFrom)
        self.assertEqual([cqlToExpression("*"), cqlToExpression("prefix:field=value")], cq.queriesFor('defaultCore'))
        self.assertEqual(1, cq.numberOfUsedCores)

    def testDedup(self):
        consume(self.tree.any.executeQuery(cqlAbstractSyntaxTree=parseCQL('*'), extraArguments={}, facets=[]))
        self.assertEqual(['executeComposedQuery'], self.observer.calledMethodNames())
        cq = self.observer.calledMethods[0].kwargs['query']
        self.assertEqual("__key__.dedup", cq.dedupField)
        self.assertEqual("__key__.date", cq.dedupSortField)

    def testNoDedupWhenDedupByDefaultSetToFalse(self):
        self.setupDna(dedupByDefault=False)
        consume(self.tree.any.executeQuery(cqlAbstractSyntaxTree=parseCQL('*'), extraArguments={}, facets=[]))
        self.assertEqual(['executeComposedQuery'], self.observer.calledMethodNames())
        cq = self.observer.calledMethods[0].kwargs['query']
        self.assertEqual(None, cq.dedupField)
        self.assertEqual(None, cq.dedupSortField)

    def testDedupExplicitlyOn(self):
        self.setupDna(dedupByDefault=False)
        consume(self.tree.any.executeQuery(cqlAbstractSyntaxTree=parseCQL('*'), extraArguments={'x-filter-common-keys': ['true']}, facets=[]))
        self.assertEqual(['executeComposedQuery'], self.observer.calledMethodNames())
        cq = self.observer.calledMethods[0].kwargs['query']
        self.assertEqual("__key__.dedup", cq.dedupField)
        self.assertEqual("__key__.date", cq.dedupSortField)

    def testXFilterCommonKeysIgnoredWhenNoDedupFieldSpecified(self):
        self.setupDna(dedupFieldName=None)
        consume(self.tree.any.executeQuery(cqlAbstractSyntaxTree=parseCQL('*'),
                extraArguments={'x-filter-common-keys': []}, facets=[]))
        self.assertEqual(['executeComposedQuery'], self.observer.calledMethodNames())
        cq = self.observer.calledMethods[0].kwargs['query']
        self.assertEqual(None, cq.dedupField)
        self.assertEqual(None, cq.dedupSortField)

    def testDedupTurnedOff(self):
        consume(self.tree.any.executeQuery(cqlAbstractSyntaxTree=parseCQL('*'), extraArguments={'x-filter-common-keys': ['false']}, facets=[]))
        self.assertEqual(['executeComposedQuery'], self.observer.calledMethodNames())
        cq = self.observer.calledMethods[0].kwargs['query']
        self.assertEqual(None, cq.dedupField)
        self.assertEqual(None, cq.dedupSortField)

    def testDedupFieldParamSet(self):
        self.setupDna(dedupByDefault=False)
        consume(self.tree.any.executeQuery(cqlAbstractSyntaxTree=parseCQL('*'), extraArguments={'x-filter-common-keys-field': ['dcterms:isFormatOf.uri']}, facets=[]))
        self.assertEqual(['executeComposedQuery'], self.observer.calledMethodNames())
        cq = self.observer.calledMethods[0].kwargs['query']
        self.assertEqual(None, cq.dedupField)
        self.assertEqual(None, cq.dedupSortField)

    def testXFilterCommonKeysFieldOverridesXFilterCommonKeys(self):
        self.setupDna(dedupByDefault=False)
        consume(self.tree.any.executeQuery(cqlAbstractSyntaxTree=parseCQL('*'), extraArguments={'x-filter-common-keys-field': ['dcterms:isFormatOf.uri'], 'x-filter-common-keys': ['true']}, facets=[]))
        self.assertEqual(['executeComposedQuery'], self.observer.calledMethodNames())
        cq = self.observer.calledMethods[0].kwargs['query']
        self.assertEqual("__key__.dcterms:isFormatOf.uri", cq.dedupField)
        self.assertEqual("__key__.date", cq.dedupSortField)

    def testClusteringNotEnabledIfTurnedOffInConfig(self):
        consume(self.tree.any.updateConfig(config=dict(features_disabled=['clustering']), indexConfig={}))
        consume(self.tree.any.executeQuery(cqlAbstractSyntaxTree=parseCQL('fiets'), extraArguments={'x-clustering': ['true']}, facets=[]))
        self.assertEqual(['executeComposedQuery'], self.observer.calledMethodNames())
        cq = self.observer.calledMethods[0].kwargs['query']
        self.assertEqual(None, cq.clusteringConfig)

    def testNormalQueryWithPossibleJoin(self):
        ast = parseCQL("prefix.field=value")
        consume(self.tree.any.executeQuery(cqlAbstractSyntaxTree=ast, extraArguments={}))
        self.assertEqual(['executeComposedQuery'], self.observer.calledMethodNames())
        cq = self.observer.calledMethods[0].kwargs['query']
        cq.validate()
        self.assertEqual('defaultCore', cq.resultsFrom)
        self.assertEqual([cqlToExpression("prefix.field=value")], cq.queriesFor('defaultCore'))

    def testNormalQueryWithoutAnyJoin(self):
        ast = parseCQL("prefixfield=value")
        consume(self.tree.any.executeQuery(cqlAbstractSyntaxTree=ast, extraArguments={}))
        self.assertEqual(['executeComposedQuery'], self.observer.calledMethodNames())
        cq = self.observer.calledMethods[0].kwargs['query']
        cq.validate()
        self.assertEqual('defaultCore', cq.resultsFrom)
        self.assertEqual([cqlToExpression("prefixfield=value")], cq.queriesFor('defaultCore'))

    def testXTermDrilldown(self):
        self.response = LuceneResponse(drilldownData=[
                dict(fieldname='prefix:field1', terms=[dict(term='term1', count=1)]),
                dict(fieldname='unknownJoinName.field', terms=[]),
                dict(fieldname='normal:drilldown', terms=[]),
                dict(fieldname='prefix:field2', terms=[dict(term='term2', count=1)]),
            ])
        response = retval(self.tree.any.executeQuery(
                cqlAbstractSyntaxTree=parseCQL('*'),
                extraArguments={},
                facets=[
                    dict(fieldname='otherCore.prefix:field2', maxTerms=7),
                    dict(fieldname='normal:drilldown', maxTerms=11),
                    dict(fieldname='otherCore.prefix:field1', maxTerms=9),
                    dict(fieldname='unknownJoinName.field', maxTerms=12)
                ],
                someKwarg='someValue'))
        self.assertEqual(['executeComposedQuery'], self.observer.calledMethodNames())
        cq = self.observer.calledMethods[0].kwargs['query']
        cq.validate()
        self.assertEqual(set(['defaultCore', 'otherCore']), cq.cores)
        self.assertEqual('keyDefault', cq.keyName('defaultCore', 'otherCore'))
        self.assertEqual('keyOther', cq.keyName('otherCore', 'defaultCore'))
        self.assertEqual([dict(fieldname='prefix:field2', path=[], maxTerms=7), dict(fieldname='prefix:field1', path=[], maxTerms=9)], cq.facetsFor('otherCore'))
        self.assertEqual([dict(fieldname='normal:drilldown', path=[], maxTerms=11), dict(fieldname='unknownJoinName.field', path=[], maxTerms=12)], cq.facetsFor('defaultCore'))
        self.assertEqual([
                dict(fieldname='otherCore.prefix:field2', terms=[dict(term='term2', count=1)]),
                dict(fieldname='normal:drilldown', terms=[]),
                dict(fieldname='otherCore.prefix:field1', terms=[dict(term='term1', count=1)]),
                dict(fieldname='unknownJoinName.field', terms=[])
            ], response.drilldownData)

    def testHierarchicalXTermDrilldown(self):
        self.response = LuceneResponse(drilldownData=[
            dict(fieldname='field1', path=['field2'], terms=[dict(term='term1', count=1)]),
        ])

        response = retval(self.tree.any.executeQuery(
            cqlAbstractSyntaxTree=parseCQL('*'),
            extraArguments={},
            facets=[
                dict(fieldname='otherCore.field1>field2', maxTerms=10),
            ],
            someKwarg='someValue'))
        self.assertEqual(['executeComposedQuery'], self.observer.calledMethodNames())
        cq = self.observer.calledMethods[0].kwargs['query']
        cq.validate()
        self.assertEqual([{'fieldname': 'field1', 'path': ['field2'], 'maxTerms': 10}], cq.facetsFor('otherCore'))
        self.assertEqual([
            dict(fieldname='otherCore.field1', path=['field2'], terms=[dict(term='term1', count=1)]),
        ], response.drilldownData)

    def testXRankQuery(self):
        consume(self.tree.any.executeQuery(cqlAbstractSyntaxTree=parseCQL('*'), extraArguments={'x-rank-query': ['otherCore.prefix:field=value', 'otherCore.otherprefix:otherfield=othervalue', 'field=value']}))
        self.assertEqual(['executeComposedQuery'], self.observer.calledMethodNames())
        cq = self.observer.calledMethods[0].kwargs['query']
        cq.validate()
        self.assertEqual(set(['defaultCore', 'otherCore']), cq.cores)
        self.assertEqual('keyDefault', cq.keyName('defaultCore', 'otherCore'))
        self.assertEqual('keyOther', cq.keyName('otherCore', 'defaultCore'))
        self.assertEqual(cqlToExpression("prefix:field=value OR otherprefix:otherfield=othervalue"), cq.rankQueryFor('otherCore'))
        self.assertEqual(cqlToExpression("field=value"), cq.rankQueryFor('defaultCore'))
        self.assertEqual([], cq.queriesFor('otherCore'))
        self.assertEqual([cqlToExpression('*')], cq.queriesFor('defaultCore'))

    def testDrilldownQueries(self):
        consume(self.tree.any.executeQuery(cqlAbstractSyntaxTree=parseCQL('*'), extraArguments={}, drilldownQueries=[('field', ['path1', 'path2'])]))
        self.assertEqual(['executeComposedQuery'], self.observer.calledMethodNames())
        cq = self.observer.calledMethods[0].kwargs['query']
        cq.validate()
        self.assertEqual([('field', ['path1', 'path2'])], cq.drilldownQueriesFor('defaultCore'))

    def testDrilldownQueriesForOtherCore(self):
        consume(self.tree.any.executeQuery(cqlAbstractSyntaxTree=parseCQL('*'), extraArguments={}, drilldownQueries=[('otherCore.field', ['path1', 'path2'])]))
        self.assertEqual(['executeComposedQuery'], self.observer.calledMethodNames())
        cq = self.observer.calledMethods[0].kwargs['query']
        cq.validate()
        self.assertEqual([('field', ['path1', 'path2'])], cq.drilldownQueriesFor('otherCore'))

    def testDrilldownQueriesWithTranslate(self):
        consume(self.tree.any.executeQuery(cqlAbstractSyntaxTree=parseCQL('*'), extraArguments={}, drilldownQueries=[('toBePrefixed', ['path1', 'path2']), ('otherCore.toBePrefixed', ['path3'])]))
        self.assertEqual(['executeComposedQuery'], self.observer.calledMethodNames())
        cq = self.observer.calledMethods[0].kwargs['query']
        cq.validate()
        self.assertEqual([('prefix.toBePrefixed', ['path1', 'path2'])], cq.drilldownQueriesFor('defaultCore'))
        self.assertEqual([('prefix.toBePrefixed', ['path3'])], cq.drilldownQueriesFor('otherCore'))

    def testClustering(self):
        consume(self.tree.any.executeQuery(cqlAbstractSyntaxTree=parseCQL('*'), extraArguments={'x-clustering': ['true']}))
        cq = self.observer.calledMethods[0].kwargs['query']
        self.assertTrue(cq.clustering)

    def testIgnoreCorePrefixForResultCore(self):
        consume(self.tree.any.executeQuery(cqlAbstractSyntaxTree=parseCQL('defaultCore.field=value')))
        cq = self.observer.calledMethods[0].kwargs['query']
        self.assertEqual([cqlToExpression('defaultCore.field=value')], cq.queriesFor('defaultCore'))

    def testSortKeys(self):
        consume(self.tree.any.executeQuery(cqlAbstractSyntaxTree=parseCQL('*'), sortKeys=[dict(sortBy='field', sortDescending=True), dict(sortBy='otherCore.field', sortDescending=False)]))
        self.assertEqual(['executeComposedQuery'], self.observer.calledMethodNames())
        cq = self.observer.calledMethods[0].kwargs['query']
        cq.validate()
        self.assertEqual(set(['defaultCore', 'otherCore']), cq.cores)
        self.assertEqual('keyDefault', cq.keyName('defaultCore', 'otherCore'))
        self.assertEqual('keyOther', cq.keyName('otherCore', 'defaultCore'))
        self.assertEqual([
                dict(sortBy='field', sortDescending=True, core='defaultCore'),
                dict(sortBy='field', sortDescending=False, core='otherCore')
            ], cq.sortKeys)

    def testConvertJoinQueryToFilters(self):
        consume(self.tree.any.executeQuery(cqlAbstractSyntaxTree=parseCQL('field=value AND otherCore.field=value2')))
        self.assertEqual(['executeComposedQuery'], self.observer.calledMethodNames())
        cq = self.observer.calledMethods[0].kwargs['query']
        self.assertEqual(QueryExpression.searchterm('field', '=', 'value'), cq.queryFor('defaultCore'))
        self.assertEqual([QueryExpression.searchterm('field', '=', 'value2')], cq.filterQueriesFor('otherCore'))

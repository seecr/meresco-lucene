## begin license ##
#
# "NBC+" also known as "ZP (ZoekPlatform)" is
#  a project of the Koninklijke Bibliotheek
#  and provides a search service for all public
#  libraries in the Netherlands.
#
# Copyright (C) 2013-2015 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2013-2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
# Copyright (C) 2015 Koninklijke Bibliotheek (KB) http://www.kb.nl
#
# This file is part of "NBC+ (Zoekplatform BNL)"
#
# "NBC+ (Zoekplatform BNL)" is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# "NBC+ (Zoekplatform BNL)" is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with "NBC+ (Zoekplatform BNL)"; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
#
## end license ##

from seecr.test import SeecrTestCase, CallTrace
from weightless.core import be
from meresco.core import Observable
from cqlparser import parseString as parseCQL
from meresco.lucene import LuceneResponse
from seecr.utils.generatorutils import generatorReturn, returnValueFromGenerator, consume
from meresco.lucene.converttocomposedquery import ConvertToComposedQuery


class ConvertToComposedQueryTest(SeecrTestCase):
    def setUp(self):
        SeecrTestCase.setUp(self)
        self.setupDna()

    def setupDna(self, dedupFieldName="__key__", dedupSortFieldName="__key__.date", groupingFieldName="__key__"):
        self.observer = CallTrace('observer', emptyGeneratorMethods=['executeComposedQuery'])
        self.response = LuceneResponse()
        def executeComposedQuery(*args, **kwargs):
            generatorReturn(self.response)
            yield
        self.observer.methods['executeComposedQuery'] = executeComposedQuery
        self.tree = be(
            (Observable(),
                (ConvertToComposedQuery(
                        resultsFrom='defaultCore',
                        matches=[(
                            dict(core='defaultCore', uniqueKey='keyDefault'),
                            dict(core='otherCore', key='keyOther')
                        )],
                        dedupFieldName=dedupFieldName,
                        dedupSortFieldName=dedupSortFieldName,
                        groupingFieldName=groupingFieldName,
                        drilldownFieldnamesTranslate=lambda name: 'prefix.' + name if name == 'toBePrefixed' else name,
                        clusterFieldNames=['title', 'creator', 'contributor']
                    ),
                    (self.observer,)
                )
            )
        )

    def testXFilter(self):
        consume(self.tree.any.executeQuery(cqlAbstractSyntaxTree=parseCQL('*'), extraArguments={'x-filter': ['otherCore.prefix:field=value']}, facets=[], start=1))
        self.assertEquals(['executeComposedQuery'], self.observer.calledMethodNames())
        cq = self.observer.calledMethods[0].kwargs['query']
        cq.validate()
        self.assertEquals(1, cq.start)
        self.assertEquals(set(['defaultCore', 'otherCore']), cq.cores)
        self.assertEquals('keyDefault', cq.keyName('defaultCore'))
        self.assertEquals('keyOther', cq.keyName('otherCore'))
        self.assertEquals([parseCQL("prefix:field=value")], cq.queriesFor('otherCore'))
        self.assertEquals([parseCQL('*')], cq.queriesFor('defaultCore'))

    def testMatchesOptional(self):
        self.tree = be(
            (Observable(),
                (ConvertToComposedQuery(resultsFrom='defaultCore'),
                    (self.observer,)
                )
            )
        )
        consume(self.tree.any.executeQuery(cqlAbstractSyntaxTree=parseCQL('*'), extraArguments={'x-filter': ['prefix:field=value']}, facets=[], start=1))
        self.assertEquals(['executeComposedQuery'], self.observer.calledMethodNames())
        cq = self.observer.calledMethods[0].kwargs['query']
        cq.validate()
        self.assertEquals(1, cq.start)
        self.assertEquals(set(['defaultCore']), cq.cores)
        self.assertRaises(KeyError, lambda: cq.keyName('defaultCore'))
        self.assertEquals([parseCQL("*"), parseCQL("prefix:field=value")], cq.queriesFor('defaultCore'))

    def testTwoXFiltersForSameCore(self):
        consume(self.tree.any.executeQuery(cqlAbstractSyntaxTree=parseCQL('*'), extraArguments={'x-filter': ['otherCore.prefix:field=value', 'otherCore.field2=value2']}, facets=[]))
        self.assertEquals(['executeComposedQuery'], self.observer.calledMethodNames())
        cq = self.observer.calledMethods[0].kwargs['query']
        cq.validate()
        self.assertEquals('defaultCore', cq.resultsFrom)
        self.assertEquals(set(['defaultCore', 'otherCore']), cq.cores)
        self.assertEquals('keyDefault', cq.keyName('defaultCore'))
        self.assertEquals('keyOther', cq.keyName('otherCore'))
        self.assertEquals([parseCQL("prefix:field=value"), parseCQL('field2=value2')], cq.queriesFor('otherCore'))

    def testXFilterWhichIsNoJoinQuery(self):
        consume(self.tree.any.executeQuery(cqlAbstractSyntaxTree=parseCQL('*'), extraArguments={'x-filter': ['prefix:field=value']}, facets=[], someKwarg='someValue'))
        self.assertEquals(['executeComposedQuery'], self.observer.calledMethodNames())
        cq = self.observer.calledMethods[0].kwargs['query']
        cq.validate()
        self.assertEquals('defaultCore', cq.resultsFrom)
        self.assertEquals([parseCQL("*"), parseCQL("prefix:field=value")], cq.queriesFor('defaultCore'))
        self.assertEquals(1, cq.numberOfUsedCores)

    def testDedup(self):
        consume(self.tree.any.executeQuery(cqlAbstractSyntaxTree=parseCQL('*'), extraArguments={}, facets=[]))
        self.assertEquals(['executeComposedQuery'], self.observer.calledMethodNames())
        cq = self.observer.calledMethods[0].kwargs['query']
        self.assertEquals("__key__", cq.otherKwargs().get("dedupField"))
        self.assertEquals("__key__.date", cq.otherKwargs().get("dedupSortField", "not there"))

    def testGroupingDefaultTurnedOff(self):
        consume(self.tree.any.executeQuery(cqlAbstractSyntaxTree=parseCQL('*'), extraArguments={}, facets=[]))
        self.assertEquals(['executeComposedQuery'], self.observer.calledMethodNames())
        cq = self.observer.calledMethods[0].kwargs['query']
        self.assertEquals(None, cq.otherKwargs().get("groupingField"))

    def testGroupingTurnedOn(self):
        consume(self.tree.any.executeQuery(cqlAbstractSyntaxTree=parseCQL('*'), extraArguments={'x-grouping': ['true']}, facets=[]))
        self.assertEquals(['executeComposedQuery'], self.observer.calledMethodNames())
        cq = self.observer.calledMethods[0].kwargs['query']
        self.assertEquals("__key__", cq.otherKwargs().get("groupingField"))

    def testXFilterCommonKeysIgnoredWhenNoDedupFieldSpecified(self):
        self.setupDna(dedupFieldName=None)
        consume(self.tree.any.executeQuery(cqlAbstractSyntaxTree=parseCQL('*'),
                extraArguments={'x-filter-common-keys': []}, facets=[]))
        self.assertEquals(['executeComposedQuery'], self.observer.calledMethodNames())
        cq = self.observer.calledMethods[0].kwargs['query']
        self.assertEquals(None, cq.otherKwargs().get("dedupField"))
        self.assertEquals(None, cq.otherKwargs().get("dedupSortField"))

    def testDedupTurnedOff(self):
        consume(self.tree.any.executeQuery(cqlAbstractSyntaxTree=parseCQL('*'), extraArguments={'x-filter-common-keys': ['false']}, facets=[]))
        self.assertEquals(['executeComposedQuery'], self.observer.calledMethodNames())
        cq = self.observer.calledMethods[0].kwargs['query']
        self.assertEquals(None, cq.otherKwargs().get("dedupField", "not there"))
        self.assertEquals(None, cq.otherKwargs().get("dedupSortField", "not there"))

    def testDetectQueryIsJoinQuery(self):
        ast = parseCQL("otherCore.prefix:field=value")
        consume(self.tree.any.executeQuery(cqlAbstractSyntaxTree=ast, extraArguments={}))
        self.assertEquals(['executeComposedQuery'], self.observer.calledMethodNames())
        cq = self.observer.calledMethods[0].kwargs['query']
        cq.validate()
        self.assertEquals('defaultCore', cq.resultsFrom)
        self.assertEquals([parseCQL("prefix:field=value")], cq.queriesFor('otherCore'))
        self.assertEquals([], cq.queriesFor('defaultCore'))

    def testNormalQueryWithPossibleJoin(self):
        ast = parseCQL("prefix.field=value")
        consume(self.tree.any.executeQuery(cqlAbstractSyntaxTree=ast, extraArguments={}))
        self.assertEquals(['executeComposedQuery'], self.observer.calledMethodNames())
        cq = self.observer.calledMethods[0].kwargs['query']
        cq.validate()
        self.assertEquals('defaultCore', cq.resultsFrom)
        self.assertEquals([parseCQL("prefix.field=value")], cq.queriesFor('defaultCore'))

    def testNormalQueryWithoutAnyJoin(self):
        ast = parseCQL("prefixfield=value")
        consume(self.tree.any.executeQuery(cqlAbstractSyntaxTree=ast, extraArguments={}))
        self.assertEquals(['executeComposedQuery'], self.observer.calledMethodNames())
        cq = self.observer.calledMethods[0].kwargs['query']
        cq.validate()
        self.assertEquals('defaultCore', cq.resultsFrom)
        self.assertEquals([parseCQL("prefixfield=value")], cq.queriesFor('defaultCore'))

    def testXTermDrilldown(self):
        self.response = LuceneResponse(drilldownData=[
                dict(fieldname='prefix:field1', terms=[dict(term='term1', count=1)]),
                dict(fieldname='prefix:field2', terms=[dict(term='term2', count=1)]),
                dict(fieldname='normal:drilldown', terms=[]),
                dict(fieldname='unknownJoinName.field', terms=[])
            ])
        response = returnValueFromGenerator(self.tree.any.executeQuery(
                cqlAbstractSyntaxTree=parseCQL('*'),
                extraArguments={},
                facets=[
                    dict(fieldname='otherCore.prefix:field1', maxTerms=9),
                    dict(fieldname='otherCore.prefix:field2', maxTerms=7),
                    dict(fieldname='normal:drilldown', maxTerms=11),
                    dict(fieldname='unknownJoinName.field', maxTerms=12)
                ],
                someKwarg='someValue'))
        self.assertEquals(['executeComposedQuery'], self.observer.calledMethodNames())
        cq = self.observer.calledMethods[0].kwargs['query']
        cq.validate()
        self.assertEquals(set(['defaultCore', 'otherCore']), cq.cores)
        self.assertEquals('keyDefault', cq.keyName('defaultCore'))
        self.assertEquals('keyOther', cq.keyName('otherCore'))
        self.assertEquals([dict(fieldname='prefix:field1', path=[], maxTerms=9), dict(fieldname='prefix:field2', path=[], maxTerms=7)], cq.facetsFor('otherCore'))
        self.assertEquals([dict(fieldname='normal:drilldown', path=[], maxTerms=11), dict(fieldname='unknownJoinName.field', path=[], maxTerms=12)], cq.facetsFor('defaultCore'))
        self.assertEquals([
                dict(fieldname='otherCore.prefix:field1', terms=[dict(term='term1', count=1)]),
                dict(fieldname='otherCore.prefix:field2', terms=[dict(term='term2', count=1)]),
                dict(fieldname='normal:drilldown', terms=[]),
                dict(fieldname='unknownJoinName.field', terms=[])
            ], response.drilldownData)

    def testHierarchicalXTermDrilldown(self):
        self.response = LuceneResponse(drilldownData=[
                dict(fieldname='field1', path=['field2'], terms=[dict(term='term1', count=1)]),
            ])

        response = returnValueFromGenerator(self.tree.any.executeQuery(
                cqlAbstractSyntaxTree=parseCQL('*'),
                extraArguments={},
                facets=[
                    dict(fieldname='otherCore.field1>field2', maxTerms=10),
                ],
                someKwarg='someValue'))
        self.assertEquals(['executeComposedQuery'], self.observer.calledMethodNames())
        cq = self.observer.calledMethods[0].kwargs['query']
        cq.validate()
        self.assertEquals([{'fieldname': 'field1', 'path': ['field2'], 'maxTerms': 10}], cq.facetsFor('otherCore'))
        self.assertEquals([
                dict(fieldname='otherCore.field1', path=['field2'], terms=[dict(term='term1', count=1)]),
            ], response.drilldownData)

    def testXRankQuery(self):
        consume(self.tree.any.executeQuery(cqlAbstractSyntaxTree=parseCQL('*'), extraArguments={'x-rank-query': ['otherCore.prefix:field=value', 'otherCore.otherprefix:otherfield=othervalue', 'defaultCore.field=value']}))
        self.assertEquals(['executeComposedQuery'], self.observer.calledMethodNames())
        cq = self.observer.calledMethods[0].kwargs['query']
        cq.validate()
        self.assertEquals(set(['defaultCore', 'otherCore']), cq.cores)
        self.assertEquals('keyDefault', cq.keyName('defaultCore'))
        self.assertEquals('keyOther', cq.keyName('otherCore'))
        self.assertEquals(parseCQL("prefix:field=value OR otherprefix:otherfield=othervalue"), cq.rankQueryFor('otherCore'))
        self.assertEquals(parseCQL("field=value"), cq.rankQueryFor('defaultCore'))
        self.assertEquals([], cq.queriesFor('otherCore'))
        self.assertEquals([parseCQL('*')], cq.queriesFor('defaultCore'))

    def testDrilldownQueries(self):
        consume(self.tree.any.executeQuery(cqlAbstractSyntaxTree=parseCQL('*'), extraArguments={}, drilldownQueries=[('field', ['path1', 'path2'])]))
        self.assertEquals(['executeComposedQuery'], self.observer.calledMethodNames())
        cq = self.observer.calledMethods[0].kwargs['query']
        cq.validate()
        self.assertEquals([('field', ['path1', 'path2'])], cq.drilldownQueriesFor('defaultCore'))

    def testDrilldownQueriesForOtherCore(self):
        consume(self.tree.any.executeQuery(cqlAbstractSyntaxTree=parseCQL('*'), extraArguments={}, drilldownQueries=[('otherCore.field', ['path1', 'path2'])]))
        self.assertEquals(['executeComposedQuery'], self.observer.calledMethodNames())
        cq = self.observer.calledMethods[0].kwargs['query']
        cq.validate()
        self.assertEquals([('field', ['path1', 'path2'])], cq.drilldownQueriesFor('otherCore'))

    def testDrilldownQueriesWithTranslate(self):
        consume(self.tree.any.executeQuery(cqlAbstractSyntaxTree=parseCQL('*'), extraArguments={}, drilldownQueries=[('toBePrefixed', ['path1', 'path2']), ('otherCore.toBePrefixed', ['path3'])]))
        self.assertEquals(['executeComposedQuery'], self.observer.calledMethodNames())
        cq = self.observer.calledMethods[0].kwargs['query']
        cq.validate()
        self.assertEquals([('prefix.toBePrefixed', ['path1', 'path2'])], cq.drilldownQueriesFor('defaultCore'))
        self.assertEquals([('prefix.toBePrefixed', ['path3'])], cq.drilldownQueriesFor('otherCore'))

    def testClustering(self):
        consume(self.tree.all.updateConfig(indexConfig=dict(clustering={'title': 1.5, 'creator': 1.1})))
        consume(self.tree.any.executeQuery(cqlAbstractSyntaxTree=parseCQL('*'), extraArguments={'x-clustering': ['true']}))
        cq = self.observer.calledMethods[0].kwargs['query']
        self.assertEqual([
                ('title', 1.5),
                ('creator', 1.1),
                ('contributor', 1.0),
            ], cq.clusterFields)

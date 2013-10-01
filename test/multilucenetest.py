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

from seecr.test import SeecrTestCase, CallTrace
from seecr.utils.generatorutils import returnValueFromGenerator, consume
from org.meresco.lucene import KeyCollector, KeyFilterCollector, DocIdCollector
from org.apache.lucene.search import MatchAllDocsQuery

from meresco.core import Observable
from weightless.core import be, compose

from meresco.lucene import Lucene
from meresco.lucene.utils import KEY_PREFIX
from meresco.lucene.multilucene import MultiLucene
from meresco.lucene.composedquery import ComposedQuery
from os.path import join
from cqlparser import parseString as parseCql
from meresco.lucene.lucenequerycomposer import LuceneQueryComposer
from lucenetest import createDocument, createCategories
from time import sleep


class MultiLuceneTest(SeecrTestCase):
    def setUp(self):
        SeecrTestCase.setUp(self)
        self.luceneA = Lucene(join(self.tempdir, 'a'), name='coreA', reactor=CallTrace(), commitCount=1)
        self.luceneB = Lucene(join(self.tempdir, 'b'), name='coreB', reactor=CallTrace(), commitCount=1)
        self.dna = be((Observable(),
            (MultiLucene(defaultCore='coreA'),
                (self.luceneA,),
                (self.luceneB,),
            )
        ))

        # +---------------------------------+     +---------------------------------+
        # |              ______             |     |                                 |
        # |         ____/      \____     A  |     |    __________                B  |
        # |        /   /\   Q  /\   \       |     |   /    N     \                  |
        # |       /   /  \    /  \   \      |     |  /   ____     \                 |
        # |      /   |    \  /    |   \     |     | |   /    \     |                |
        # |     /     \    \/    /     \    |     | |  |  M __|____|_____           |
        # |    /       \   /\   /       \   |     | |   \__/_/     |     \          |
        # |   |         \_|__|_/         |  |     |  \    |       /      |          |
        # |   |    U      |  |     M     |  |     |   \___|______/    ___|_______   |
        # |   |           \  /           |  |     |       |          /   |       \  |
        # |    \           \/           /   |     |       |   O     /   _|__      \ |
        # |     \          /\          /    |     |        \_______|___/_/  \     | |
        # |      \        /  \        /     |     |                |  |  M   | P  | |
        # |       \______/    \______/      |     |                |   \____/     | |
        # |                                 |     |                 \            /  |
        # |                                 |     |                  \__________/   |
        # +---------------------------------+     +---------------------------------+

        k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11 = range(1,12)
        self.addDocument(self.luceneA, identifier='A',      keys=[('A', k1 )], fields=[('M', 'false'), ('Q', 'false'), ('U', 'false'), ('S', '1')])
        self.addDocument(self.luceneA, identifier='A-U',    keys=[('A', k2 )], fields=[('M', 'false'), ('Q', 'false'), ('U', 'true' ), ('S', '2')])
        self.addDocument(self.luceneA, identifier='A-Q',    keys=[('A', k3 )], fields=[('M', 'false'), ('Q', 'true' ), ('U', 'false'), ('S', '3')])
        self.addDocument(self.luceneA, identifier='A-QU',   keys=[('A', k4 )], fields=[('M', 'false'), ('Q', 'true' ), ('U', 'true' ), ('S', '4')])
        self.addDocument(self.luceneA, identifier='A-M',    keys=[('A', k5 )], fields=[('M', 'true' ), ('Q', 'false'), ('U', 'false'), ('S', '5')])
        self.addDocument(self.luceneA, identifier='A-MU',   keys=[('A', k6 )], fields=[('M', 'true' ), ('Q', 'false'), ('U', 'true' ), ('S', '6')])
        self.addDocument(self.luceneA, identifier='A-MQ',   keys=[('A', k7 )], fields=[('M', 'true' ), ('Q', 'true' ), ('U', 'false'), ('S', '7')])
        self.addDocument(self.luceneA, identifier='A-MQU',  keys=[('A', k8 )], fields=[('M', 'true' ), ('Q', 'true' ), ('U', 'true' ), ('S', '8')])

        self.addDocument(self.luceneB, identifier='B-N>A-M',   keys=[('B', k5 )], fields=[('N', 'true' ), ('O', 'true' ), ('P', 'false')])
        self.addDocument(self.luceneB, identifier='B-N>A-MU',  keys=[('B', k6 )], fields=[('N', 'true' ), ('O', 'false'), ('P', 'false')])
        self.addDocument(self.luceneB, identifier='B-N>A-MQ',  keys=[('B', k7 )], fields=[('N', 'true' ), ('O', 'true' ), ('P', 'false')])
        self.addDocument(self.luceneB, identifier='B-N>A-MQU', keys=[('B', k8 )], fields=[('N', 'true' ), ('O', 'false'), ('P', 'false')])
        self.addDocument(self.luceneB, identifier='B-N',       keys=[('B', k9 )], fields=[('N', 'true' ), ('O', 'true' ), ('P', 'false')])
        self.addDocument(self.luceneB, identifier='B',         keys=[('B', k10)], fields=[('N', 'false'), ('O', 'false'), ('P', 'false')])
        self.addDocument(self.luceneB, identifier='B-P>A-M',   keys=[('B', k5 )], fields=[('N', 'false'), ('O', 'true' ), ('P', 'true' )])
        self.addDocument(self.luceneB, identifier='B-P>A-MU',  keys=[('B', k6 )], fields=[('N', 'false'), ('O', 'false'), ('P', 'true' )])
        self.addDocument(self.luceneB, identifier='B-P>A-MQ',  keys=[('B', k7 )], fields=[('N', 'false'), ('O', 'true' ), ('P', 'true' )])
        self.addDocument(self.luceneB, identifier='B-P>A-MQU', keys=[('B', k8 )], fields=[('N', 'false'), ('O', 'false'), ('P', 'true' )])
        self.addDocument(self.luceneB, identifier='B-P',       keys=[('B', k11)], fields=[('N', 'false'), ('O', 'true' ), ('P', 'true' )])
        sleep(0.2)

    def tearDown(self):
        self.luceneA.finish()
        self.luceneB.finish()
        SeecrTestCase.tearDown(self)

    def testQueryOneIndex(self):
        result = returnValueFromGenerator(self.dna.any.executeQuery(luceneQuery=query('Q=true')))
        self.assertEquals(set(['A-Q', 'A-QU', 'A-MQ', 'A-MQU']), set(result.hits))
        result = returnValueFromGenerator(self.dna.any.executeQuery(luceneQuery=query('Q=true AND M=true')))
        self.assertEquals(set(['A-MQ', 'A-MQU']), set(result.hits))

    def testQueryOneIndexWithComposedQuery(self):
        cq = ComposedQuery()
        cq.add(core='coreA', query=query('Q=true'))
        cq.resultsFrom = 'coreA'
        result = returnValueFromGenerator(self.dna.any.executeComposedQuery(cq))
        self.assertEquals(set(['A-Q', 'A-QU', 'A-MQ', 'A-MQU']), set(result.hits))
        cq = ComposedQuery()
        cq.add(core='coreA', query=query('Q=true'), filterQueries=[query('M=true')])
        cq.resultsFrom = 'coreA'
        result = returnValueFromGenerator(self.dna.any.executeComposedQuery(cq))
        self.assertEquals(set(['A-MQ', 'A-MQU']), set(result.hits))

    def testB_N_is_true(self):
        result = returnValueFromGenerator(self.dna.any.executeQuery(core='coreB', luceneQuery=query('N=true')))
        self.assertEquals(5, result.total)
        self.assertEquals(set(['B-N', 'B-N>A-M', 'B-N>A-MU', 'B-N>A-MQ', 'B-N>A-MQU']), set(result.hits))

    def testJoinQuery(self):
        q = ComposedQuery()
        q.add(core='coreA')
        q.add(core='coreB', query=query('N=true'))
        q.resultsFrom = 'coreA'
        q.addMatch(coreA=KEY_PREFIX+'A', coreB=KEY_PREFIX+'B')
        result = returnValueFromGenerator(self.dna.any.executeComposedQuery(q))
        self.assertEquals(4, result.total)
        self.assertEquals(set(['A-M', 'A-MU', 'A-MQ', 'A-MQU']), set(result.hits))

    def testJoinQueryWithFilters(self):
        q = ComposedQuery()
        q.add(core='coreA')
        q.add(core='coreB', filterQueries=[query('N=true')])
        q.resultsFrom = 'coreA'
        q.addMatch(coreA=KEY_PREFIX+'A', coreB=KEY_PREFIX+'B')
        result = returnValueFromGenerator(self.dna.any.executeComposedQuery(q))
        self.assertEquals(4, result.total)
        self.assertEquals(set(['A-M', 'A-MU', 'A-MQ', 'A-MQU']), set(result.hits))

    def testNotSupportedComposedQueries(self):
        try:
            consume(self.dna.any.executeComposedQuery(query=ComposedQuery()))
            self.fail()
        except ValueError, e:
            self.assertTrue('Unsupported' in str(e), str(e))

    def testJoinFacet(self):
        q = ComposedQuery()
        q.add(core='coreA', query=query('Q=true'))
        q.add(core='coreB', query=None, facets=[
                dict(fieldname='cat_N', maxTerms=10),
                dict(fieldname='cat_O', maxTerms=10),
            ])
        q.resultsFrom = 'coreA'
        q.addMatch(coreA=KEY_PREFIX + 'A', coreB=KEY_PREFIX + 'B')
        result = returnValueFromGenerator(self.dna.any.executeComposedQuery(query=q))
        self.assertEquals(4, result.total)
        self.assertEquals([{
                'terms': [
                        {'count': 2, 'term': u'false'},
                        {'count': 2, 'term': u'true'},
                    ],
                'fieldname': u'cat_N'
            }, {
                'terms': [
                    {'count': 2, 'term': u'false'},
                    {'count': 2, 'term': u'true'},
                ],
                'fieldname': u'cat_O'
            }], result.drilldownData)

    def testJoinFacetFromBPointOfView(self):
        q = ComposedQuery()
        q.add(core='coreA', query=query('Q=true'))
        q.add(core='coreB', query=None, facets=[
                dict(fieldname='cat_N', maxTerms=10),
                dict(fieldname='cat_O', maxTerms=10),
            ])
        q.resultsFrom = 'coreB'
        q.addMatch(coreA=KEY_PREFIX + 'A', coreB=KEY_PREFIX + 'B')
        result = returnValueFromGenerator(self.dna.any.executeComposedQuery(query=q))
        self.assertEquals(4, result.total)
        self.assertEquals(set(['B-N>A-MQ', 'B-N>A-MQU', 'B-P>A-MQ', 'B-P>A-MQU']), set(result.hits))
        self.assertEquals([{
                'terms': [
                        {'count': 2, 'term': u'false'},
                        {'count': 2, 'term': u'true'},
                    ],
                'fieldname': u'cat_N'
            }, {
                'terms': [
                    {'count': 2, 'term': u'false'},
                    {'count': 2, 'term': u'true'},
                ],
                'fieldname': u'cat_O'
             }], result.drilldownData)

    def testJoinFacetWillNotFilter(self):
        query = ComposedQuery()
        query.add(core='coreA', query=None)
        query.add(core='coreB', query=None, facets=[
                dict(fieldname='cat_N', maxTerms=10),
            ])
        query.resultsFrom = 'coreA'
        query.addMatch(coreA=KEY_PREFIX + 'A', coreB=KEY_PREFIX + 'B')
        result = returnValueFromGenerator(self.dna.any.executeComposedQuery(query=query))
        self.assertEquals(8, result.total)
        self.assertEquals([{
                'terms': [
                    {'count': 4, 'term': u'false'},
                    {'count': 4, 'term': u'true'},
                ],
                'fieldname': u'cat_N'
            }], result.drilldownData)

    def testJoinFacetAndQuery(self):
        q = ComposedQuery()
        q.add(core='coreA', query=None)
        q.add(core='coreB', query=query('N=true'), facets=[
                dict(fieldname='cat_N', maxTerms=10),
                dict(fieldname='cat_O', maxTerms=10),
            ])
        q.resultsFrom = 'coreA'
        q.addMatch(coreA=KEY_PREFIX + 'A', coreB=KEY_PREFIX + 'B')
        result = returnValueFromGenerator(self.dna.any.executeComposedQuery(query=q))
        self.assertEquals(4, result.total)
        self.assertEquals(set(['A-M', 'A-MU', 'A-MQ', 'A-MQU']), set(result.hits))
        self.assertEquals([{
                'terms': [
                    {'count': 4, 'term': u'true'},
                ],
                'fieldname': u'cat_N'
            }, {
                'terms': [
                    {'count': 2, 'term': u'false'},
                    {'count': 2, 'term': u'true'},
                ],
                'fieldname': u'cat_O'
            }], result.drilldownData)

    def testCoreInfo(self):
        infos = list(compose(self.dna.all.coreInfo()))
        self.assertEquals(2, len(infos))

    def testUniteResultForJustOneIndex(self):
        result = returnValueFromGenerator(self.dna.any.executeQuery(core='coreA', luceneQuery=query('(Q=true AND U=true) OR (Q=true AND M=true)')))
        self.assertEquals(3, result.total)
        self.assertEquals(set(['A-QU', 'A-MQ', 'A-MQU']), set(result.hits))

    def testUniteResultFromTwoIndexes(self):
        q = ComposedQuery()
        q.add(core='coreA', query=query('Q=true'))
        q.add(core='coreB', query=None)
        q.resultsFrom = 'coreA'
        q.addMatch(coreA=KEY_PREFIX+'A', coreB=KEY_PREFIX+'B')
        q.unite(coreA=query('U=true'), coreB=query('N=true'))
        result = returnValueFromGenerator(self.dna.any.executeComposedQuery(q))
        self.assertEquals(3, result.total)
        self.assertEquals(set(['A-QU', 'A-MQ', 'A-MQU']), set(result.hits))

    def testUniteResultFromTwoIndexes_filterQueries(self):
        q = ComposedQuery()
        q.add(core='coreA', query=None, filterQueries=[query('Q=true')])
        q.add(core='coreB', query=None)
        q.resultsFrom = 'coreA'
        q.addMatch(coreA=KEY_PREFIX+'A', coreB=KEY_PREFIX+'B')
        q.unite(coreA=query('U=true'), coreB=query('N=true'))
        result = returnValueFromGenerator(self.dna.any.executeComposedQuery(q))
        self.assertEquals(3, result.total)
        self.assertEquals(set(['A-QU', 'A-MQ', 'A-MQU']), set(result.hits))

    def testUniteAndFacets(self):
        q = ComposedQuery()
        q.add(core='coreA', query=query('Q=true'), facets=[
                dict(fieldname='cat_Q', maxTerms=10),
                dict(fieldname='cat_U', maxTerms=10),
            ])
        q.add(core='coreB', query=None, facets=[
                dict(fieldname='cat_N', maxTerms=10),
                dict(fieldname='cat_O', maxTerms=10),
            ])
        q.resultsFrom = 'coreA'
        q.addMatch(coreA=KEY_PREFIX+'A', coreB=KEY_PREFIX+'B')
        q.unite(coreA=query('U=true'), coreB=query('N=true'))
        result = returnValueFromGenerator(self.dna.any.executeComposedQuery(q))
        self.assertEquals(3, result.total)
        self.assertEquals([{
                'terms': [
                    {'count': 3, 'term': u'true'},
                ],
                'fieldname': u'cat_Q'
            }, {
                'terms': [
                    {'count': 2, 'term': u'true'},
                    {'count': 1, 'term': u'false'},
                ],
                'fieldname': u'cat_U'
            }, {
                'terms': [
                    {'count': 2, 'term': u'true'},
                ],
                'fieldname': u'cat_N'
            }, {
                'terms': [
                    {'count': 1, 'term': u'false'},
                    {'count': 1, 'term': u'true'},
                ],
                'fieldname': u'cat_O'
            }], result.drilldownData)

    def not_yet_implemented_testUniteCoreB(self):
        q = ComposedQuery()
        q.resultsFrom = 'coreB'
        q.addMatch(coreA=KEY_PREFIX +'A', coreB=KEY_PREFIX +'B')
        q.unite(coreA=query('U=true'), coreB=query('N=true'))
        result = returnValueFromGenerator(self.dna.any.executeComposedQuery(q))
        self.assertEquals(['B-N', 'B-N>A-M', 'B-N>A-MQ', 'B-N>A-MQU', 'B-N>A-MU', 'B-P>A-MQU', 'B-P>A-MU', ], sorted(result.hits))

    def testReminderThatCollectingNonUniqueKeysGivesTooManyResults(self):
        keyBCollector = KeyCollector(KEY_PREFIX + 'B')
        consume(self.luceneB.search(query=query('N=true'), collector=keyBCollector))
        keyBSet = keyBCollector.getKeySet()
        result = returnValueFromGenerator(self.luceneB.executeQuery(
                MatchAllDocsQuery(),
                filterCollector=KeyFilterCollector(keyBSet, KEY_PREFIX + 'B'),
            ))
        self.assertEquals(['B-N', 'B-N>A-M', 'B-N>A-MQ', 'B-N>A-MQU', 'B-N>A-MU', 'B-P>A-M', 'B-P>A-MQ', 'B-P>A-MQU', 'B-P>A-MU'], sorted(result.hits))

    def testNto1UniteExperiment(self):
        # unite B-N met A-U, query op A-Q en not B-O
        uniteDocIdCollector = DocIdCollector()
        consume(self.luceneB.search(query=query('N=true and O=false'), collector=uniteDocIdCollector))
        matchKeyCollector = KeyCollector(KEY_PREFIX + 'A')
        consume(self.luceneA.search(query=query('U=true and Q=true'), collector=matchKeyCollector))
        keyFilterCollector = KeyFilterCollector(matchKeyCollector.getKeySet(), KEY_PREFIX + 'B')
        keyFilterCollector.setDelegate(uniteDocIdCollector)
        consume(self.luceneB.search(query=query('O=false'), collector=keyFilterCollector))

        matchKeyCollector = KeyCollector(KEY_PREFIX + 'A')
        consume(self.luceneA.search(query=query('Q=true'), collector=matchKeyCollector))
        keyFilterCollector = KeyFilterCollector(matchKeyCollector.getKeySet(), KEY_PREFIX + 'B')
        result = returnValueFromGenerator(self.luceneB.executeQuery(luceneQuery=query('O=false'), filter=uniteDocIdCollector.getDocIdFilter(), filterCollector=keyFilterCollector))             
        self.assertEquals([u'B-N>A-MQU', u'B-P>A-MQU'], sorted(result.hits))

    def not_yet_implemented_testUniteAndFacetsCoreB(self):
        q = ComposedQuery()
        q.add(core='coreA', query=query('Q=true'))
        q.add(core='coreB', query=None, facets=[
                dict(fieldname='cat_N', maxTerms=10),
                dict(fieldname='cat_O', maxTerms=10),
            ])
        q.resultsFrom = 'coreB'
        q.addMatch(coreA=KEY_PREFIX + 'A', coreB=KEY_PREFIX + 'B')
        q.unite(coreA=query('U=true'), coreB=query('N=true'))
        result = returnValueFromGenerator(self.dna.any.executeComposedQuery(q))
        self.assertEquals(set(['B-N>A-MQ', 'B-N>MQU']), set(result.hits))
        self.assertEquals(2, result.total)
        self.assertEquals([{
                'terms': [
                    {'count': 2, 'term': u'true'},
                ],
                'fieldname': u'cat_N'
            }, {
                'terms': [
                    {'count': 1, 'term': u'false'},
                    {'count': 1, 'term': u'true'},
                ],
                'fieldname': u'cat_O'
            }], result.drilldownData)

    def testUniteAndFacetsWithForeignQuery(self):
        q = ComposedQuery()
        q.add(core='coreA', query=None)
        q.add(core='coreB', query=query('O=true'), facets=[
                dict(fieldname='cat_N', maxTerms=10),
                dict(fieldname='cat_O', maxTerms=10),
            ])
        q.resultsFrom = 'coreA'
        q.addMatch(coreA=KEY_PREFIX+'A', coreB=KEY_PREFIX+'B')
        q.unite(coreA=query('U=true'), coreB=query('N=true'))
        result = returnValueFromGenerator(self.dna.any.executeComposedQuery(q))
        self.assertEquals(2, result.total)
        self.assertEquals([{
               'terms': [
                    {'count': 2, 'term': u'true'},
                ],
                'fieldname': u'cat_N'
            }, {
                'terms': [
                    {'count': 2, 'term': u'true'},
                ],
                'fieldname': u'cat_O'
            }], result.drilldownData)

    def testUniteMakesItTwoCoreQuery(self):
        q = ComposedQuery()
        q.add(core='coreA', query=query('Q=true'))
        q.resultsFrom = 'coreA'
        q.addMatch(coreA=KEY_PREFIX+'A', coreB=KEY_PREFIX+'B')
        q.unite(coreA=query('U=true'), coreB=query('N=true'))
        result = returnValueFromGenerator(self.dna.any.executeComposedQuery(q))
        self.assertEquals(3, result.total)
        self.assertEquals(set(['A-QU', 'A-MQ', 'A-MQU']), set(result.hits))

    def testStartStopSortKeys(self):
        q = ComposedQuery()
        q.add(core='coreA', query=query('Q=true'))
        q.add(core='coreB', query=None)
        q.resultsFrom = 'coreA'
        q.addMatch(coreA=KEY_PREFIX+'A', coreB=KEY_PREFIX+'B')
        q.unite(coreA=query('U=true'), coreB=query('N=true'))
        q.sortKeys=[dict(sortBy='S', sortDescending=False)]
        result = returnValueFromGenerator(self.dna.any.executeComposedQuery(q))
        self.assertEquals(3, result.total)
        self.assertEquals(['A-QU', 'A-MQ', 'A-MQU'], result.hits)
        q.sortKeys=[dict(sortBy='S', sortDescending=True)]
        result = returnValueFromGenerator(self.dna.any.executeComposedQuery(q))
        self.assertEquals(3, result.total)
        self.assertEquals(['A-MQU', 'A-MQ', 'A-QU'], result.hits)
        q.stop = 2
        result = returnValueFromGenerator(self.dna.any.executeComposedQuery(q))
        self.assertEquals(3, result.total)
        self.assertEquals(['A-MQU', 'A-MQ'], result.hits)
        q.stop = 10
        q.start = 1
        result = returnValueFromGenerator(self.dna.any.executeComposedQuery(q))
        self.assertEquals(3, result.total)
        self.assertEquals(['A-MQ', 'A-QU'], result.hits)




    def addDocument(self, lucene, identifier, keys, fields):
        consume(lucene.addDocument(
            identifier=identifier,
            document=createDocument([(KEY_PREFIX + keyField, keyValue) for (keyField, keyValue) in keys]+fields),
            categories=createCategories([('cat_'+field, value) for field, value in fields])
            ))

def query(cqlString):
    return LuceneQueryComposer([]).compose(parseCql(cqlString))

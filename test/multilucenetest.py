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
        # |        /   /\   Q  /\   \       |     |   /          \                  |
        # |       /   /  \    /  \   \      |     |  /   ____     \                 |
        # |      /   |    \  /    |   \     |     | |   /    \     |                |
        # |     /     \    \/    /     \    |     | |  |  M   | N  |                |
        # |    /       \   /\   /       \   |     | |   \____/     |                |
        # |   |         \_|__|_/         |  |     |  \            /                 |
        # |   |    U      |  |     M     |  |     |   \__________/                  |
        # |   |           \  /           |  |     |                                 |
        # |    \           \/           /   |     |                                 |
        # |     \          /\          /    |     |                                 |
        # |      \        /  \        /     |     |                                 |
        # |       \______/    \______/      |     |                                 |
        # |                                 |     |                                 |
        # |                                 |     |                                 |
        # +---------------------------------+     +---------------------------------+

        k1, k2, k3, k4, k5, k6, k7, k8, k9, k10 = range(1,11)
        self.addDocument(self.luceneA, identifier='A',      key=('A', k1 ), fields=[('M', 'false'), ('Q', 'false'), ('U', 'false'), ('S', '1')])
        self.addDocument(self.luceneA, identifier='A-U',    key=('A', k2 ), fields=[('M', 'false'), ('Q', 'false'), ('U', 'true' ), ('S', '2')])
        self.addDocument(self.luceneA, identifier='A-Q',    key=('A', k3 ), fields=[('M', 'false'), ('Q', 'true' ), ('U', 'false'), ('S', '3')])
        self.addDocument(self.luceneA, identifier='A-QU',   key=('A', k4 ), fields=[('M', 'false'), ('Q', 'true' ), ('U', 'true' ), ('S', '4')])
        self.addDocument(self.luceneA, identifier='A-M',    key=('A', k5 ), fields=[('M', 'true' ), ('Q', 'false'), ('U', 'false'), ('S', '5')])
        self.addDocument(self.luceneA, identifier='A-MU',   key=('A', k6 ), fields=[('M', 'true' ), ('Q', 'false'), ('U', 'true' ), ('S', '6')])
        self.addDocument(self.luceneA, identifier='A-MQ',   key=('A', k7 ), fields=[('M', 'true' ), ('Q', 'true' ), ('U', 'false'), ('S', '7')])
        self.addDocument(self.luceneA, identifier='A-MQU',  key=('A', k8 ), fields=[('M', 'true' ), ('Q', 'true' ), ('U', 'true' ), ('S', '8')])

        self.addDocument(self.luceneB, identifier='B-N>A-M',   key=('B', k5 ), fields=[('N', 'true' ), ('O', 'true' )])
        self.addDocument(self.luceneB, identifier='B-N>A-MU',  key=('B', k6 ), fields=[('N', 'true' ), ('O', 'false')])
        self.addDocument(self.luceneB, identifier='B-N>A-MQ',  key=('B', k7 ), fields=[('N', 'true' ), ('O', 'true' )])
        self.addDocument(self.luceneB, identifier='B-N>A-MQU', key=('B', k8 ), fields=[('N', 'true' ), ('O', 'false')])
        self.addDocument(self.luceneB, identifier='B-N',       key=('B', k9 ), fields=[('N', 'true' ), ('O', 'true' )])
        self.addDocument(self.luceneB, identifier='B',         key=('B', k10), fields=[('N', 'false'), ('O', 'false')])
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

    def testB_N_is_true(self):
        result = returnValueFromGenerator(self.dna.any.executeQuery(core='coreB', luceneQuery=query('N=true')))
        self.assertEquals(5, result.total)
        self.assertEquals(set(['B-N', 'B-N>A-M', 'B-N>A-MU', 'B-N>A-MQ', 'B-N>A-MQU']), set(result.hits))

    def testJoinQuery(self):
        q = ComposedQuery()
        q.add(core='coreA')
        q.add(core='coreB', query=query('N=true'))
        q.resultsFrom('coreA')
        q.addMatch(coreA=KEY_PREFIX+'A', coreB=KEY_PREFIX+'B')
        result = returnValueFromGenerator(self.dna.any.executeComposedQuery(q))
        self.assertEquals(4, result.total)
        self.assertEquals(set(['A-M', 'A-MU', 'A-MQ', 'A-MQU']), set(result.hits))

    def testJoinQueryWithFilters(self):
        q = ComposedQuery()
        q.add(core='coreA')
        q.add(core='coreB', filterQueries=[query('N=true')])
        q.resultsFrom('coreA')
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
        q.resultsFrom('coreA')
        q.addMatch(coreA=KEY_PREFIX + 'A', coreB=KEY_PREFIX + 'B')
        result = returnValueFromGenerator(self.dna.any.executeComposedQuery(query=q))
        self.assertEquals(4, result.total)
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

    def testJoinFacetFromBPointOfView(self):
        q = ComposedQuery()
        q.add(core='coreA', query=query('Q=true'))
        q.add(core='coreB', query=None, facets=[
                dict(fieldname='cat_N', maxTerms=10),
                dict(fieldname='cat_O', maxTerms=10),
            ])
        q.resultsFrom('coreB')
        q.addMatch(coreA=KEY_PREFIX + 'A', coreB=KEY_PREFIX + 'B')
        result = returnValueFromGenerator(self.dna.any.executeComposedQuery(query=q))
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

    def testJoinFacetWillNotFilter(self):
        query = ComposedQuery()
        query.add(core='coreA', query=None)
        query.add(core='coreB', query=None, facets=[
                dict(fieldname='cat_N', maxTerms=10),
            ])
        query.resultsFrom('coreA')
        query.addMatch(coreA=KEY_PREFIX + 'A', coreB=KEY_PREFIX + 'B')
        result = returnValueFromGenerator(self.dna.any.executeComposedQuery(query=query))
        self.assertEquals(8, result.total)
        self.assertEquals([{
                'terms': [
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
        q.resultsFrom('coreA')
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
        q.resultsFrom('coreA')
        q.addMatch(coreA=KEY_PREFIX+'A', coreB=KEY_PREFIX+'B')
        q.unite(coreA=query('U=true'), coreB=query('N=true'))
        result = returnValueFromGenerator(self.dna.any.executeComposedQuery(q))
        self.assertEquals(3, result.total)
        self.assertEquals(set(['A-QU', 'A-MQ', 'A-MQU']), set(result.hits))

    def testUniteResultFromTwoIndexes_filterQueries(self):
        q = ComposedQuery()
        q.add(core='coreA', query=None, filterQueries=[query('Q=true')])
        q.add(core='coreB', query=None)
        q.resultsFrom('coreA')
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
            ])
        q.resultsFrom('coreA')
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
            }], result.drilldownData)

    def testStartStopSortKeys(self):
        q = ComposedQuery()
        q.add(core='coreA', query=query('Q=true'))
        q.add(core='coreB', query=None)
        q.resultsFrom('coreA')
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




    def addDocument(self, lucene, identifier, key, fields):
        keyField, keyValue = key
        consume(lucene.addDocument(
            identifier=identifier,
            document=createDocument([(KEY_PREFIX + keyField, keyValue)]+fields),
            categories=createCategories([('cat_'+field, value) for field, value in fields])
            ))

def query(cqlString):
    return LuceneQueryComposer([]).compose(parseCql(cqlString))

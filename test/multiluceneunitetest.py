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
from meresco.lucene.multiquery import MultiQuery
from os.path import join
from org.apache.lucene.search import TermQuery, MatchAllDocsQuery
from org.apache.lucene.index import Term
from cqlparser import parseString as parseCql
from meresco.lucene.lucenequerycomposer import LuceneQueryComposer
from lucenetest import createDocument, createCategories
from time import sleep


class MultiLuceneUniteTest(SeecrTestCase):
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
        consume(self.luceneA.addDocument('A',      document=createDocument([(KEY_PREFIX + 'A', k1 ), ('M', 'false'), ('Q', 'false'), ('U', 'false')])))
        consume(self.luceneA.addDocument('A-U',    document=createDocument([(KEY_PREFIX + 'A', k2 ), ('M', 'false'), ('Q', 'false'), ('U', 'true' )])))
        consume(self.luceneA.addDocument('A-Q',    document=createDocument([(KEY_PREFIX + 'A', k3 ), ('M', 'false'), ('Q', 'true' ), ('U', 'false')])))
        consume(self.luceneA.addDocument('A-QU',   document=createDocument([(KEY_PREFIX + 'A', k4 ), ('M', 'false'), ('Q', 'true' ), ('U', 'true' )])))
        consume(self.luceneA.addDocument('A-M',    document=createDocument([(KEY_PREFIX + 'A', k5 ), ('M', 'true' ), ('Q', 'false'), ('U', 'false')])))
        consume(self.luceneA.addDocument('A-MU',   document=createDocument([(KEY_PREFIX + 'A', k6 ), ('M', 'true' ), ('Q', 'false'), ('U', 'true' )])))
        consume(self.luceneA.addDocument('A-MQ',   document=createDocument([(KEY_PREFIX + 'A', k7 ), ('M', 'true' ), ('Q', 'true' ), ('U', 'false')])))
        consume(self.luceneA.addDocument('A-MQU',  document=createDocument([(KEY_PREFIX + 'A', k8 ), ('M', 'true' ), ('Q', 'true' ), ('U', 'true' )])))

        consume(self.luceneB.addDocument('B-N>A-M',   document=createDocument([(KEY_PREFIX + 'B', k5 ), ('N', 'true' )])))
        consume(self.luceneB.addDocument('B-N>A-MU',  document=createDocument([(KEY_PREFIX + 'B', k6 ), ('N', 'true' )])))
        consume(self.luceneB.addDocument('B-N>A-MQ',  document=createDocument([(KEY_PREFIX + 'B', k7 ), ('N', 'true' )])))
        consume(self.luceneB.addDocument('B-N>A-MQU', document=createDocument([(KEY_PREFIX + 'B', k8 ), ('N', 'true' )])))
        consume(self.luceneB.addDocument('B-N',       document=createDocument([(KEY_PREFIX + 'B', k9 ), ('N', 'true' )])))
        consume(self.luceneB.addDocument('B',         document=createDocument([(KEY_PREFIX + 'B', k10), ('N', 'false')])))
        sleep(0.5)

    def tearDown(self):
        self.luceneA.finish()
        self.luceneB.finish()
        SeecrTestCase.tearDown(self)

    def testSetup(self):
        result = returnValueFromGenerator(self.dna.any.executeQuery(luceneQuery=query('Q=true')))
        self.assertEquals(set(['A-Q', 'A-QU', 'A-MQ', 'A-MQU']), set(result.hits))
        result = returnValueFromGenerator(self.dna.any.executeQuery(luceneQuery=query('Q=true AND M=true')))
        self.assertEquals(set(['A-MQ', 'A-MQU']), set(result.hits))

    def testB_N_is_true(self):
        result = returnValueFromGenerator(self.dna.any.executeQuery(core='coreB', luceneQuery=query('N=true')))
        self.assertEquals(5, result.total)
        self.assertEquals(set(['B-N', 'B-N>A-M', 'B-N>A-MU', 'B-N>A-MQ', 'B-N>A-MQU']), set(result.hits))

    def testA_where_B_N_is_true(self):
        q = MultiQuery()
        q.add(core='coreA')
        q.add(core='coreB', query=query('N=true'))
        q.resultsFrom('coreA')
        q.addMatch(coreA=KEY_PREFIX+'A', coreB=KEY_PREFIX+'B')
        result = returnValueFromGenerator(self.dna.any.executeMultiQuery(q))
        self.assertEquals(4, result.total)
        self.assertEquals(set(['A-M', 'A-MU', 'A-MQ', 'A-MQU']), set(result.hits))

    def testUniteResultForJustOneIndex(self):
        result = returnValueFromGenerator(self.dna.any.executeQuery(core='coreA', luceneQuery=query('(Q=true AND U=true) OR (Q=true AND M=true)')))
        self.assertEquals(3, result.total)
        self.assertEquals(set(['A-QU', 'A-MQ', 'A-MQU']), set(result.hits))

    def testUniteResultFromTwoIndexes(self):
        q = MultiQuery()
        q.add(core='coreA', query=query('Q=true'))
        q.add(core='coreB', query=None)
        q.resultsFrom('coreA')
        q.addMatch(coreA=KEY_PREFIX+'A', coreB=KEY_PREFIX+'B')
        q.unite(coreA=query('U=true'), coreB=query('N=true'))
        result = returnValueFromGenerator(self.dna.any.executeMultiQuery(q))
        self.assertEquals(3, result.total)
        self.assertEquals(set(['A-QU', 'A-MQ', 'A-MQU']), set(result.hits))


def query(cqlString):
    return LuceneQueryComposer([]).compose(parseCql(cqlString))

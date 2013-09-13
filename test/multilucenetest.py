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
from seecr.utils.generatorutils import returnValueFromGenerator

from meresco.core import Observable
from weightless.core import be, compose

from meresco.lucene import Lucene
from meresco.lucene.utils import KEY_PREFIX
from meresco.lucene.multilucene import MultiLucene
from os.path import join
from org.apache.lucene.search import TermQuery, MatchAllDocsQuery
from org.apache.lucene.index import Term

from lucenetest import createDocument, createCategories
from time import sleep
from random import randint


class MultiLuceneTest(SeecrTestCase):
    def setUp(self):
        SeecrTestCase.setUp(self)
        self.luceneA = Lucene(join(self.tempdir, 'a'), name='A', reactor=CallTrace(), commitCount=1)
        self.luceneB = Lucene(join(self.tempdir, 'b'), name='B', reactor=CallTrace(), commitCount=1)
        self.luceneC = Lucene(join(self.tempdir, 'c'), name='C', reactor=CallTrace(), commitCount=1)
        self.dna = be((Observable(),
            (MultiLucene(defaultCore='A'),
                (self.luceneA,),
                (self.luceneB,),
                (self.luceneC,),
            )
        ))
        key1 = 1
        key2 = 2
        key3 = 3
        key4 = 4
        returnValueFromGenerator(self.luceneA.addDocument(identifier="id:0", document=createDocument([(KEY_PREFIX + 'A', key1), ('field1', 'value0')]), categories=createCategories([('field1', 'first item0')])))
        returnValueFromGenerator(self.luceneA.addDocument(identifier="id:1", document=createDocument([(KEY_PREFIX + 'A', key2), ('field1', 'value0')]), categories=createCategories([('field1', 'first item1')])))
        returnValueFromGenerator(self.luceneA.addDocument(identifier="id:2", document=createDocument([(KEY_PREFIX + 'A', key3), ('field1', 'value1')]), categories=createCategories([('field1', 'first item1')])))
        returnValueFromGenerator(self.luceneA.addDocument(identifier="id:7", document=createDocument([(KEY_PREFIX + 'A', key4), ('field1', 'value1')])))
        returnValueFromGenerator(self.luceneB.addDocument(identifier="id:3", document=createDocument([(KEY_PREFIX + 'B', key1), ('field2', 'value1')]), categories=createCategories([('field2', 'first item2')])))
        returnValueFromGenerator(self.luceneB.addDocument(identifier="id:4", document=createDocument([(KEY_PREFIX + 'B', key2), ('field2', 'value2'), ('field3', 'value3')]), categories=createCategories([('field2', 'first item3'), ('field3', 'first')])))
        returnValueFromGenerator(self.luceneB.addDocument(identifier="id:5", document=createDocument([(KEY_PREFIX + 'B', key3), ('field2', 'value1'), ('field3', 'value3')]), categories=createCategories([('field2', 'first item3'), ('field3', 'second')])))
        returnValueFromGenerator(self.luceneC.addDocument(identifier="id:6", document=createDocument([(KEY_PREFIX + 'C', key1), ('field4', 'value4')])))
        sleep(0.2)

    def tearDown(self):
        self.luceneA.finish()
        self.luceneB.finish()
        self.luceneC.finish()
        SeecrTestCase.tearDown(self)

    def testQueryOneIndex(self):
        result = returnValueFromGenerator(self.dna.any.executeQuery(
                luceneQuery=TermQuery(Term("field1", "value0")),
            ))
        self.assertEquals(2, result.total)
        self.assertEquals(set(['id:0', 'id:1']), set(result.hits))
        result = returnValueFromGenerator(self.dna.any.executeQuery(
                luceneQuery=MatchAllDocsQuery(),
            ))
        self.assertEquals(4, result.total)
        self.assertEquals(set(['id:0', 'id:1', 'id:2', 'id:7']), set(result.hits))
        result = returnValueFromGenerator(self.dna.any.executeQuery(
                luceneQuery=MatchAllDocsQuery(),
                core='B',
            ))
        self.assertEquals(3, result.total)
        self.assertEquals(set(['id:3', 'id:4', 'id:5']), set(result.hits))

    def testJoinQuery(self):
        result = returnValueFromGenerator(self.dna.any.executeQuery(
                luceneQuery=MatchAllDocsQuery(),
                core='A',
                joins={'A': KEY_PREFIX + 'A', 'B': KEY_PREFIX + 'B'},
                joinQueries={'B': TermQuery(Term('field2', 'value1'))}
            ))
        self.assertEquals(['id:0', 'id:2'], result.hits)
        self.assertTrue(result.queryTime > 0, result.asJson())

    def testMultipleJoinQueries(self):
        self.assertRaises(ValueError, lambda: returnValueFromGenerator(self.dna.any.executeQuery(
                    luceneQuery=TermQuery(Term("field1", "value0")),
                    core='A',
                    joins={'A': KEY_PREFIX + 'A', 'B': KEY_PREFIX + 'B', 'C': KEY_PREFIX + 'C'},
                    joinQueries={
                        'B': TermQuery(Term('field2', 'value1')),
                        'C': TermQuery(Term('field4', 'value4'))
                    },
                )))

    def testJoinFacet(self):
        result = returnValueFromGenerator(self.dna.any.executeQuery(
                luceneQuery=TermQuery(Term("field1", "value0")),
                joins={'A': KEY_PREFIX + 'A', 'B': KEY_PREFIX + 'B'},
                core='A',
                joinFacets={'B': [
                    dict(fieldname='field2', maxTerms=10),
                    dict(fieldname='field3', maxTerms=10)
                ]}
            ))
        self.assertEquals(2, result.total)
        self.assertEquals([{
                'terms': [
                    {'count': 1, 'term': u'first item3'},
                    {'count': 1, 'term': u'first item2'},
                ],
                'fieldname': u'field2'
            }, {
                'terms': [
                    {'count': 1, 'term': u'first'},
                ],
                'fieldname': u'field3'
            }], result.drilldownData)

    def testJoinFacetWillNotFilter(self):
        result = returnValueFromGenerator(self.dna.any.executeQuery(
                luceneQuery=MatchAllDocsQuery(),
                joins={'A': KEY_PREFIX + 'A', 'B': KEY_PREFIX + 'B'},
                core='A',
                joinFacets={'B': [
                    dict(fieldname='field3', maxTerms=10),
                ]}
            ))
        self.assertEquals(4, result.total)
        self.assertEquals(['id:0', 'id:1', 'id:2', 'id:7'], result.hits)
        self.assertEquals([{
                'terms': [
                    {'count': 1, 'term': u'second'},
                    {'count': 1, 'term': u'first'},
                ],
                'fieldname': u'field3'
            }], result.drilldownData)

    def testJoinFacetAndQuery(self):
        result = returnValueFromGenerator(self.dna.any.executeQuery(
                luceneQuery=MatchAllDocsQuery(),
                core='A',
                joins={'A': KEY_PREFIX + 'A', 'B': KEY_PREFIX + 'B'},
                joinQueries={'B': TermQuery(Term('field2', 'value1'))},
                joinFacets={'B': [
                    dict(fieldname='field2', maxTerms=10),
                    dict(fieldname='field3', maxTerms=10)
                ]}
            ))
        self.assertEquals(['id:0', 'id:2'], result.hits)
        self.assertEquals([{
                'terms': [
                    {'count': 1, 'term': u'first item3'},
                    {'count': 1, 'term': u'first item2'},
                ],
                'fieldname': u'field2'
            }, {
                'terms': [
                    {'count': 1, 'term': u'second'},
                ],
                'fieldname': u'field3'
            }], result.drilldownData)

    def testCoreInfo(self):
        infos = list(compose(self.dna.all.coreInfo()))
        self.assertEquals(3, len(infos))

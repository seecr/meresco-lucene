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


class MultiLuceneTest(SeecrTestCase):
    def setUp(self):
        SeecrTestCase.setUp(self)
        self.luceneA = Lucene(join(self.tempdir, 'a'), name='coreA', reactor=CallTrace(), commitCount=1)
        self.luceneB = Lucene(join(self.tempdir, 'b'), name='coreB', reactor=CallTrace(), commitCount=1)
        self.luceneC = Lucene(join(self.tempdir, 'c'), name='coreC', reactor=CallTrace(), commitCount=1)
        self.dna = be((Observable(),
            (MultiLucene(defaultCore='coreA'),
                (self.luceneA,),
                (self.luceneB,),
                (self.luceneC,),
            )
        ))
        key1 = 1
        key2 = 2
        key3 = 3
        key4 = 4
        returnValueFromGenerator(self.luceneA.addDocument(identifier="id:0", document=createDocument([(KEY_PREFIX + 'A', key1), ('field1', 'value0')]), categories=createCategories([('cat1', 'cat1 0')])))
        returnValueFromGenerator(self.luceneA.addDocument(identifier="id:1", document=createDocument([(KEY_PREFIX + 'A', key2), ('field1', 'value0')]), categories=createCategories([('cat1', 'cat1 1')])))
        returnValueFromGenerator(self.luceneA.addDocument(identifier="id:2", document=createDocument([(KEY_PREFIX + 'A', key3), ('field1', 'value1')]), categories=createCategories([('cat1', 'cat1 1')])))
        returnValueFromGenerator(self.luceneA.addDocument(identifier="id:7", document=createDocument([(KEY_PREFIX + 'A', key4), ('field1', 'value1')])))
        returnValueFromGenerator(self.luceneB.addDocument(identifier="id:3", document=createDocument([(KEY_PREFIX + 'B', key1), ('field2', 'value1')]), categories=createCategories([('cat2', 'cat2 2')])))
        returnValueFromGenerator(self.luceneB.addDocument(identifier="id:4", document=createDocument([(KEY_PREFIX + 'B', key2), ('field2', 'value2'), ('field3', 'value3')]), categories=createCategories([('cat2', 'cat2 3'), ('cat3', 'cat3 0')])))
        returnValueFromGenerator(self.luceneB.addDocument(identifier="id:5", document=createDocument([(KEY_PREFIX + 'B', key3), ('field2', 'value1'), ('field3', 'value3')]), categories=createCategories([('cat2', 'cat2 3'), ('cat3', 'cat3 1')])))
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
                core='coreB',
            ))
        self.assertEquals(3, result.total)
        self.assertEquals(set(['id:3', 'id:4', 'id:5']), set(result.hits))

    def testJoinQuery(self):
        result = returnValueFromGenerator(self.dna.any.executeMultiQuery(
                luceneQuery=MatchAllDocsQuery(),
                core='coreA',
                joins={'coreA': KEY_PREFIX + 'A', 'coreB': KEY_PREFIX + 'B'},
                joinQueries={'coreB': TermQuery(Term('field2', 'value1'))}
            ))
        self.assertEquals(['id:0', 'id:2'], result.hits)
        self.assertTrue(result.queryTime > 0, result.asJson())

    def testJoinQuery2(self):
        result = returnValueFromGenerator(self.dna.any.executeMultiQuery(
            queries={
                'coreA': {
                    'query': None, # MatchAllDocsQuery
                    'primary': True,
                },
                'coreB': {
                    'query': TermQuery(Term('field2', 'value1')),
                }
            },
            match={
                ('coreA', 'coreB'): (KEY_PREFIX + 'A', KEY_PREFIX + 'B'),
            }
        ))
        self.assertEquals(['id:0', 'id:2'], result.hits)

    def testUniteQuery(self):
        result = returnValueFromGenerator(self.dna.any.executeMultiQuery(
            queries={
                'coreA': {
                    'query': TermQuery(Term('__all__', 'fiets')),
                    'primary': True,
                    'unite': 'dcterms:source="ihlia" or dcterms:source="uitburo"',
                },
                'coreB': {
                    'query': None,
                    'unite': 'lh:holder="info:isil:NL-0800070000"',
                }
            },
            match={
                ('coreA', 'coreB'): (KEY_PREFIX + 'A', KEY_PREFIX + 'B'),
            }
        ))


    def testMultipleJoinQueries(self):
        self.assertRaises(ValueError, lambda: returnValueFromGenerator(self.dna.any.executeMultiQuery(
                    luceneQuery=TermQuery(Term("field1", "value0")),
                    core='coreA',
                    joins={'coreA': KEY_PREFIX + 'A', 'coreB': KEY_PREFIX + 'B', 'coreC': KEY_PREFIX + 'C'},
                    joinQueries={
                        'coreB': TermQuery(Term('field2', 'value1')),
                        'coreC': TermQuery(Term('field4', 'value4'))
                    },
                )))

    def testJoinFacet(self):
        result = returnValueFromGenerator(self.dna.any.executeMultiQuery(
                luceneQuery=TermQuery(Term("field1", "value0")),
                joins={'coreA': KEY_PREFIX + 'A', 'coreB': KEY_PREFIX + 'B'},
                core='coreA',
                joinFacets={'coreB': [
                    dict(fieldname='cat2', maxTerms=10),
                    dict(fieldname='cat3', maxTerms=10)
                ]}
            ))
        self.assertEquals(2, result.total)
        self.assertEquals([{
                'terms': [
                    {'count': 1, 'term': u'cat2 3'},
                    {'count': 1, 'term': u'cat2 2'},
                ],
                'fieldname': u'cat2'
            }, {
                'terms': [
                    {'count': 1, 'term': u'cat3 0'},
                ],
                'fieldname': u'cat3'
            }], result.drilldownData)

    def testJoinFacetWillNotFilter(self):
        result = returnValueFromGenerator(self.dna.any.executeMultiQuery(
                luceneQuery=MatchAllDocsQuery(),
                joins={'coreA': KEY_PREFIX + 'A', 'coreB': KEY_PREFIX + 'B'},
                core='coreA',
                joinFacets={'coreB': [
                    dict(fieldname='cat3', maxTerms=10),
                ]}
            ))
        self.assertEquals(4, result.total)
        self.assertEquals(['id:0', 'id:1', 'id:2', 'id:7'], result.hits)
        self.assertEquals([{
                'terms': [
                    {'count': 1, 'term': u'cat3 1'},
                    {'count': 1, 'term': u'cat3 0'},
                ],
                'fieldname': u'cat3'
            }], result.drilldownData)

    def testJoinFacetAndQuery(self):
        result = returnValueFromGenerator(self.dna.any.executeMultiQuery(
                luceneQuery=MatchAllDocsQuery(),
                core='coreA',
                joins={'coreA': KEY_PREFIX + 'A', 'coreB': KEY_PREFIX + 'B'},
                joinQueries={'coreB': TermQuery(Term('field2', 'value1'))},
                joinFacets={'coreB': [
                    dict(fieldname='cat2', maxTerms=10),
                    dict(fieldname='cat3', maxTerms=10)
                ]}
            ))
        self.assertEquals(['id:0', 'id:2'], result.hits)
        self.assertEquals([{
                'terms': [
                    {'count': 1, 'term': u'cat2 3'},
                    {'count': 1, 'term': u'cat2 2'},
                ],
                'fieldname': u'cat2'
            }, {
                'terms': [
                    {'count': 1, 'term': u'cat3 1'},
                ],
                'fieldname': u'cat3'
            }], result.drilldownData)

    def testCoreInfo(self):
        infos = list(compose(self.dna.all.coreInfo()))
        self.assertEquals(3, len(infos))

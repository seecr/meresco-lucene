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
from meresco.lucene.utils import JOINHASH_PREFIX
from meresco.lucene.multilucene import MultiLucene
from os.path import join
from org.apache.lucene.search import TermQuery, MatchAllDocsQuery
from org.apache.lucene.index import Term

from lucenetest import createDocument, createCategories
from time import sleep


class MultiLuceneTest(SeecrTestCase):
    def setUp(self):
        SeecrTestCase.setUp(self)
        self.luceneA = Lucene(join(self.tempdir, 'a'), name='A', reactor=CallTrace(), commitCount=1)
        self.luceneB = Lucene(join(self.tempdir, 'b'), name='B', reactor=CallTrace(), commitCount=1)
        self.dna = be((Observable(),
            (MultiLucene(defaultCore='A'),
                (self.luceneA,),
                (self.luceneB,),
            )
        ))
        returnValueFromGenerator(self.luceneA.addDocument(identifier="id:0", document=createDocument([(JOINHASH_PREFIX + 'A', '1'), ('field1', 'value0')]), categories=createCategories([('field1', 'first item0')])))
        returnValueFromGenerator(self.luceneA.addDocument(identifier="id:1", document=createDocument([(JOINHASH_PREFIX + 'A', '2'), ('field1', 'value0')]), categories=createCategories([('field1', 'first item1')])))
        returnValueFromGenerator(self.luceneA.addDocument(identifier="id:2", document=createDocument([(JOINHASH_PREFIX + 'A', '3'), ('field1', 'value1')]), categories=createCategories([('field1', 'first item1')])))
        returnValueFromGenerator(self.luceneB.addDocument(identifier="id:3", document=createDocument([(JOINHASH_PREFIX + 'B', '1'), ('field2', 'value1')]), categories=createCategories([('field2', 'first item2')])))
        returnValueFromGenerator(self.luceneB.addDocument(identifier="id:4", document=createDocument([(JOINHASH_PREFIX + 'B', '2'), ('field2', 'value2'), ('field3', 'value3')]), categories=createCategories([('field2', 'first item3'), ('field3', 'first')])))
        returnValueFromGenerator(self.luceneB.addDocument(identifier="id:5", document=createDocument([(JOINHASH_PREFIX + 'B', '2'), ('field2', 'value1'), ('field3', 'value3')]), categories=createCategories([('field2', 'first item3'), ('field3', 'second')])))
        sleep(0.2)

    def testQueryOneIndex(self):
        result = returnValueFromGenerator(self.dna.any.executeQuery(
                luceneQuery=TermQuery(Term("field1", "value0")),
            ))
        self.assertEquals(2, result.total)
        self.assertEquals(set(['id:0', 'id:1']), set(result.hits))
        result = returnValueFromGenerator(self.dna.any.executeQuery(
                luceneQuery=MatchAllDocsQuery(),
            ))
        self.assertEquals(3, result.total)
        self.assertEquals(set(['id:0', 'id:1', 'id:2']), set(result.hits))
        result = returnValueFromGenerator(self.dna.any.executeQuery(
                luceneQuery=MatchAllDocsQuery(),
                core='B',
            ))
        self.assertEquals(3, result.total)
        self.assertEquals(set(['id:3', 'id:4', 'id:5']), set(result.hits))

    def testJoinQuery(self):
        result = returnValueFromGenerator(self.dna.any.executeQuery(
                luceneQuery=TermQuery(Term("field1", "value0")),
                core='A',
                joinQueries=[{
                    'core': 'B',
                    'fromField': JOINHASH_PREFIX + 'B',
                    'toField': JOINHASH_PREFIX + 'A',
                    'luceneQuery': TermQuery(Term('field2', 'value1')),
                }]
            ))
        self.assertEquals(['id:0', 'id:1'], result.hits)
        self.assertTrue(result.queryTime > 0, result.asJson())

    def testMultipleJoinQueries(self):
        result = returnValueFromGenerator(self.dna.any.executeQuery(
                luceneQuery=TermQuery(Term("field1", "value0")),
                core='A',
                joinQueries=[{
                    'core': 'B',
                    'fromField': JOINHASH_PREFIX + 'B',
                    'toField': JOINHASH_PREFIX + 'A',
                    'luceneQuery': TermQuery(Term('field2', 'value1')),
                },{
                    'core': 'B',
                    'fromField': JOINHASH_PREFIX + 'B',
                    'toField': JOINHASH_PREFIX + 'A',
                    'luceneQuery': TermQuery(Term('field3', 'value3')),
                }]
            ))
        self.assertEquals(['id:1'], result.hits)
        self.assertTrue(result.queryTime > 0, result.asJson())

    def testJoinFacet(self):
        result = returnValueFromGenerator(self.dna.any.executeQuery(
                luceneQuery=TermQuery(Term("field1", "value0")),
                core='A',
                joinFacets=[{
                    'core': 'B',
                    'fromField': JOINHASH_PREFIX + 'B',
                    'toField': JOINHASH_PREFIX + 'A',
                    'facet': dict(fieldname='field2', maxTerms=10)
                }, {
                    'core': 'B',
                    'fromField': JOINHASH_PREFIX + 'B',
                    'toField': JOINHASH_PREFIX + 'A',
                    'facet': dict(fieldname='field3', maxTerms=10)
                }]
            ))
        self.assertEquals([{
                'terms': [
                    {'count': 2, 'term': u'first item3'},
                    {'count': 1, 'term': u'first item2'},
                ],
                'fieldname': u'field2'
            }, {
                'terms': [
                    {'count': 1, 'term': u'second'},
                    {'count': 1, 'term': u'first'},
                ],
                'fieldname': u'field3'
            }], result.drilldownData)

    def testJoinFacetAndQuery(self):
        result = returnValueFromGenerator(self.dna.any.executeQuery(
                luceneQuery=TermQuery(Term("field1", "value0")),
                core='A',
                joinQueries=[{
                    'core': 'B',
                    'fromField': JOINHASH_PREFIX + 'B',
                    'toField': JOINHASH_PREFIX + 'A',
                    'luceneQuery': TermQuery(Term('field2', 'value1')),
                }],
                joinFacets=[{
                    'core': 'B',
                    'fromField': JOINHASH_PREFIX + 'B',
                    'toField': JOINHASH_PREFIX + 'A',
                    'facet': dict(fieldname='field2', maxTerms=10)
                }, {
                    'core': 'B',
                    'fromField': JOINHASH_PREFIX + 'B',
                    'toField': JOINHASH_PREFIX + 'A',
                    'facet': dict(fieldname='field3', maxTerms=10)
                }]
            ))
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
        self.assertEquals(2, len(infos))

    def testJoinQueryIsCachedAsFilter(self):
        for i in range(1000):
            fields = [(JOINHASH_PREFIX + 'B', '1')]
            returnValueFromGenerator(self.luceneB.addDocument(identifier="id:%s" % i, document=createDocument(fields)))
        query = MatchAllDocsQuery()
        result = returnValueFromGenerator(self.dna.any.executeQuery(
                luceneQuery=MatchAllDocsQuery(),
                core='A',
                joinQueries=[{
                    'core': 'B',
                    'fromField': JOINHASH_PREFIX + 'B',
                    'toField': JOINHASH_PREFIX + 'A',
                    'luceneQuery': query,
                }]
            ))
        self.assertTrue(result.queryTime > 20, result.asJson())
        for i in xrange(20):
            result = returnValueFromGenerator(self.dna.any.executeQuery(
                    luceneQuery=MatchAllDocsQuery(),
                    core='A',
                    joinQueries=[{
                        'core': 'B',
                        'fromField': JOINHASH_PREFIX + 'B',
                        'toField': JOINHASH_PREFIX + 'A',
                        'luceneQuery': query,
                    }]
                ))
        self.assertTrue(result.queryTime < 2, result.asJson())

    def testJoinFacetCached(self):
        for i in range(1000):
            fields = [(JOINHASH_PREFIX + 'B', '1'), ('field2', 'value%s' % i), ('field3', 'value%s' % i)]
            returnValueFromGenerator(self.luceneB.addDocument(identifier="id:%s" % i, document=createDocument(fields), categories=createCategories(fields)))
        sleep(0.1)

        for i in range(100):
            returnValueFromGenerator(self.dna.any.executeQuery(
                luceneQuery=MatchAllDocsQuery(),
                core='A',
                joinFacets=[{
                    'core': 'B',
                    'fromField': JOINHASH_PREFIX + 'B',
                    'toField': JOINHASH_PREFIX + 'A',
                    'facet': dict(fieldname='field2', maxTerms=10)
                }, {
                    'core': 'B',
                    'fromField': JOINHASH_PREFIX + 'B',
                    'toField': JOINHASH_PREFIX + 'A',
                    'facet': dict(fieldname='field3', maxTerms=10)
                }]
            ))

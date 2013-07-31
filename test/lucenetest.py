# -*- encoding: utf-8 -*-
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
from os.path import join
from time import sleep
from meresco.lucene import Lucene, VM
from meresco.lucene._lucene import IDFIELD
from meresco.lucene.lucenequerycomposer import LuceneQueryComposer
from cqlparser import parseString as parseCql
from org.apache.lucene.search import MatchAllDocsQuery, TermQuery, TermRangeQuery
from org.apache.lucene.document import Document, TextField, Field
from org.apache.lucene.index import Term
from org.apache.lucene.facet.taxonomy import CategoryPath
from org.apache.lucene.search.join import TermsCollector
from seecr.utils.generatorutils import returnValueFromGenerator
import gc

class LuceneTest(SeecrTestCase):
    def setUp(self):
        super(LuceneTest, self).setUp()
        self._javaObjects = self._getJavaObjects()
        self._reactor = CallTrace('reactor')
        self.lucene = Lucene(join(self.tempdir, 'lucene'), commitCount=1, reactor=self._reactor)

    def tearDown(self):
        self._reactor.calledMethods.reset() # don't keep any references.
        self.lucene.finish()
        self.lucene = None
        gc.collect()
        diff = self._getJavaObjects() - self._javaObjects
        self.assertEquals(0, len(diff), diff)

    def _getJavaObjects(self):
        refs = VM._dumpRefs(classes=True)
        return set([(c, refs[c]) for c in refs.keys() if c != 'class java.lang.Class'])

    def testCreate(self):
        result = returnValueFromGenerator(self.lucene.executeQuery(MatchAllDocsQuery()))
        self.assertEquals(0, result.total)

    def testAdd1Document(self):
        document = Document()
        document.add(TextField('title', 'The title', Field.Store.NO))
        returnValueFromGenerator(self.lucene.addDocument(identifier="identifier", document=document))
        sleep(0.1)
        result = returnValueFromGenerator(self.lucene.executeQuery(MatchAllDocsQuery()))
        self.assertEquals(1, result.total)
        self.assertEquals(['identifier'], result.hits)
        result = returnValueFromGenerator(self.lucene.executeQuery(TermQuery(Term("title", 'title'))))
        self.assertEquals(1, result.total)
        result = returnValueFromGenerator(self.lucene.executeQuery(TermQuery(Term("title", 'the'))))
        self.assertEquals(1, result.total)
        self.assertTrue(result.queryTime > 0.0001, result.asJson())

    def testAddAndDeleteDocument(self):
        returnValueFromGenerator(self.lucene.addDocument(identifier="id:0", document=Document()))
        returnValueFromGenerator(self.lucene.addDocument(identifier="id:1", document=Document()))
        returnValueFromGenerator(self.lucene.addDocument(identifier="id:2", document=Document()))
        returnValueFromGenerator(self.lucene.delete(identifier="id:1"))
        sleep(0.1)
        result = returnValueFromGenerator(self.lucene.executeQuery(MatchAllDocsQuery()))
        self.assertEquals(2, result.total)
        self.assertEquals(set(['id:0', 'id:2']), set(result.hits))

    def testAddCommitAfterTimeout(self):
        self.lucene.finish()
        self.lucene = Lucene(join(self.tempdir, 'lucene'), reactor=self._reactor, commitTimeout=42, commitCount=3)
        returnValueFromGenerator(self.lucene.addDocument(identifier="id:0", document=Document()))
        self.assertEquals(['addTimer'], self._reactor.calledMethodNames())
        self.assertEquals(42, self._reactor.calledMethods[0].kwargs['seconds'])
        commit = self._reactor.calledMethods[0].kwargs['callback']
        self._reactor.calledMethods.reset()
        result = returnValueFromGenerator(self.lucene.executeQuery(MatchAllDocsQuery()))
        self.assertEquals(0, result.total)
        commit()
        self.assertEquals([], self._reactor.calledMethodNames())
        result = returnValueFromGenerator(self.lucene.executeQuery(MatchAllDocsQuery()))
        self.assertEquals(1, result.total)

    def testAddAndCommitCount3(self):
        self.lucene.finish()
        self.lucene = Lucene(join(self.tempdir, 'lucene'), reactor=self._reactor, commitTimeout=42, commitCount=3)
        token = object()
        self._reactor.returnValues['addTimer'] = token
        returnValueFromGenerator(self.lucene.addDocument(identifier="id:0", document=Document()))
        result = returnValueFromGenerator(self.lucene.executeQuery(MatchAllDocsQuery()))
        self.assertEquals(0, result.total)
        self.assertEquals(['addTimer'], self._reactor.calledMethodNames())
        self.assertEquals(42, self._reactor.calledMethods[0].kwargs['seconds'])

        returnValueFromGenerator(self.lucene.addDocument(identifier="id:1", document=Document()))
        result = returnValueFromGenerator(self.lucene.executeQuery(MatchAllDocsQuery()))
        self.assertEquals(0, result.total)
        self.assertEquals(['addTimer'], self._reactor.calledMethodNames())
        returnValueFromGenerator(self.lucene.addDocument(identifier="id:2", document=Document()))
        result = returnValueFromGenerator(self.lucene.executeQuery(MatchAllDocsQuery()))
        self.assertEquals(3, result.total)
        self.assertEquals(['addTimer', 'removeTimer'], self._reactor.calledMethodNames())
        self.assertEquals(token, self._reactor.calledMethods[1].kwargs['token'])

    def testAddTwiceUpdatesDocument(self):
        returnValueFromGenerator(self.lucene.addDocument(identifier="id:0", document=createDocument([
            ('field0', 'value0'),
            ('field1', 'value1'),
            ])))
        returnValueFromGenerator(self.lucene.addDocument(identifier="id:0", document=createDocument([
            ('field1', 'value1'),
            ])))
        sleep(0.1)
        result = returnValueFromGenerator(self.lucene.executeQuery(TermQuery(Term('field1', 'value1'))))
        self.assertEquals(1, result.total)
        result = returnValueFromGenerator(self.lucene.executeQuery(TermQuery(Term('field0', 'value0'))))
        self.assertEquals(0, result.total)

    def testSorting(self):
        returnValueFromGenerator(self.lucene.addDocument(identifier="id:0", document=createDocument([
                ('field0', 'AA'),
                ('field1', 'ZZ'),
                ('field2', 'AA'),
            ])))
        returnValueFromGenerator(self.lucene.addDocument(identifier="id:1", document=createDocument([
                ('field0', 'BB'),
                ('field1', 'AA'),
                ('field2', 'ZZ'),
            ])))
        returnValueFromGenerator(self.lucene.addDocument(identifier="id:2", document=createDocument([
                ('field0', 'CC'),
                ('field1', 'ZZ'),
                ('field2', 'ZZ'),
            ])))
        sleep(0.1)
        result = returnValueFromGenerator(self.lucene.executeQuery(MatchAllDocsQuery(), sortKeys=[dict(sortBy='field0', sortDescending=False)]))
        self.assertEquals(3, result.total)
        self.assertEquals(['id:0', 'id:1', 'id:2'], result.hits)
        result = returnValueFromGenerator(self.lucene.executeQuery(MatchAllDocsQuery(), sortKeys=[dict(sortBy='field0', sortDescending=True)]))
        self.assertEquals(['id:2', 'id:1', 'id:0'], result.hits)
        result = returnValueFromGenerator(self.lucene.executeQuery(MatchAllDocsQuery(), sortKeys=[dict(sortBy='field1', sortDescending=True), dict(sortBy='field0', sortDescending=True)]))
        self.assertEquals(['id:2', 'id:0', 'id:1'], result.hits)

    def testStartStop(self):
        returnValueFromGenerator(self.lucene.addDocument(identifier="id:0", document=createDocument([('field1', 'id:0')])))
        returnValueFromGenerator(self.lucene.addDocument(identifier="id:1", document=createDocument([('field1', 'id:1')])))
        returnValueFromGenerator(self.lucene.addDocument(identifier="id:2", document=createDocument([('field1', 'id:2')])))
        sleep(0.1)
        result = returnValueFromGenerator(self.lucene.executeQuery(MatchAllDocsQuery(), start=1, stop=10, sortKeys=[dict(sortBy='field1', sortDescending=False)]))
        self.assertEquals(3, result.total)
        self.assertEquals(['id:1', 'id:2'], result.hits)
        result = returnValueFromGenerator(self.lucene.executeQuery(MatchAllDocsQuery(), start=0, stop=2, sortKeys=[dict(sortBy='field1', sortDescending=False)]))
        self.assertEquals(['id:0', 'id:1'], result.hits)
        result = returnValueFromGenerator(self.lucene.executeQuery(MatchAllDocsQuery(), start=0, stop=0))
        self.assertEquals(3, result.total)
        self.assertEquals([], result.hits)
        result = returnValueFromGenerator(self.lucene.executeQuery(MatchAllDocsQuery(), start=2, stop=2))
        self.assertEquals(3, result.total)
        self.assertEquals([], result.hits)

    def testFacets(self):
        returnValueFromGenerator(self.lucene.addDocument(identifier="id:0", document=createDocument([('field1', 'id:0')]), categories=createCategories([('field2', 'first item0'), ('field3', 'second item')])))
        returnValueFromGenerator(self.lucene.addDocument(identifier="id:1", document=createDocument([('field1', 'id:1')]), categories=createCategories([('field2', 'first item1'), ('field3', 'other value')])))
        returnValueFromGenerator(self.lucene.addDocument(identifier="id:2", document=createDocument([('field1', 'id:2')]), categories=createCategories([('field2', 'first item2'), ('field3', 'second item')])))
        # does not crash!!!
        returnValueFromGenerator(self.lucene.executeQuery(MatchAllDocsQuery(), facets=[dict(maxTerms=10, fieldname='field2')]))
        sleep(0.1)
        result = returnValueFromGenerator(self.lucene.executeQuery(MatchAllDocsQuery(), facets=[dict(maxTerms=10, fieldname='field2')]))
        
        self.assertEquals([{
                'fieldname': 'field2',
                'terms': [
                    {'term': 'first item2', 'count': 1},
                    {'term': 'first item1', 'count': 1},
                    {'term': 'first item0', 'count': 1},
                ],
            }],result.drilldownData)
        result = returnValueFromGenerator(self.lucene.executeQuery(MatchAllDocsQuery(), facets=[dict(maxTerms=10, fieldname='field3')]))
        self.assertEquals([{
                'fieldname': 'field3',
                'terms': [
                    {'term': 'second item', 'count': 2},
                    {'term': 'other value', 'count': 1},
                ],
            }],result.drilldownData)

    def XX_testFacetsWithIllegalCharacters(self):
        categories = createCategories([('field', 'a/b')])
        # The following print statement causes an error to be printed to stderr.
        # It keeps on working.
        self.assertEquals('[<CategoryPath: class org.apache.lucene.facet.taxonomy.CategoryPath>]', str(categories))
        returnValueFromGenerator(self.lucene.addDocument(identifier="id:0", document=createDocument([]), categories=categories))
        sleep(0.1)
        result = returnValueFromGenerator(self.lucene.executeQuery(MatchAllDocsQuery(), facets=[dict(maxTerms=10, fieldname='field')]))
        self.assertEquals([{
                'fieldname': 'field',
                'terms': [
                    {'term': 'a/b', 'count': 1},
                ],
            }],result.drilldownData)

    def testEscapeFacets(self):
        returnValueFromGenerator(self.lucene.addDocument(identifier="id:0", document=createDocument([('field1', 'id:0')]), categories=createCategories([('field2', 'first/item0')])))
        sleep(0.1)
        result = returnValueFromGenerator(self.lucene.executeQuery(MatchAllDocsQuery(), facets=[dict(maxTerms=10, fieldname='field2')]))
        self.assertEquals([{
                'terms': [{'count': 1, 'term': u'first/item0'}],
                'fieldname': u'field2'
            }],result.drilldownData)

    def testDiacritics(self):
        returnValueFromGenerator(self.lucene.addDocument(identifier='hendrik', document=createDocument([('title', 'Waar is Morée vandaag?')])))
        sleep(0.1)
        result = returnValueFromGenerator(self.lucene.executeQuery(TermQuery(Term('title', 'moree'))))
        self.assertEquals(1, result.total)
        query = LuceneQueryComposer(unqualifiedTermFields=[]).compose(parseCql("title=morée"))
        result = returnValueFromGenerator(self.lucene.executeQuery(query))
        self.assertEquals(1, result.total)

    def testFilterQueries(self):
        for i in xrange(10):
            returnValueFromGenerator(self.lucene.addDocument(identifier="id:%s" % i, document=createDocument([
                    ('mod2', 'v%s' % (i % 2)), 
                    ('mod3', 'v%s' % (i % 3))
                ])))
        # id     0  1  2  3  4  5  6  7  8  9  
        # mod2  v0 v1 v0 v1 v0 v1 v0 v1 v0 v1
        # mod3  v0 v1 v2 v0 v1 v2 v0 v1 v2 v0
        sleep(0.1)
        result = returnValueFromGenerator(self.lucene.executeQuery(MatchAllDocsQuery(), filterQueries=[TermQuery(Term('mod2', 'v0'))]))
        self.assertEquals(5, result.total)
        self.assertEquals(['id:0', 'id:2', 'id:4', 'id:6', 'id:8'], result.hits)
        result = returnValueFromGenerator(self.lucene.executeQuery(MatchAllDocsQuery(), filterQueries=[
                TermQuery(Term('mod2', 'v0')),
                TermQuery(Term('mod3', 'v0')),
            ]))
        self.assertEquals(2, result.total)
        self.assertEquals(['id:0', 'id:6'], result.hits)

    def testPrefixSearch(self):
        response = returnValueFromGenerator(self.lucene.prefixSearch(fieldname='field1', prefix='valu'))
        self.assertEquals([], response.hits)
        returnValueFromGenerator(self.lucene.addDocument(identifier="id:0", document=createDocument([('field1', 'value0')])))
        returnValueFromGenerator(self.lucene.addDocument(identifier="id:1", document=createDocument([('field1', 'value1')])))
        returnValueFromGenerator(self.lucene.addDocument(identifier="id:2", document=createDocument([('field1', 'value1')])))
        sleep(0.1)
        response = returnValueFromGenerator(self.lucene.prefixSearch(fieldname='field1', prefix='valu'))
        self.assertEquals(['value1', 'value0'], response.hits)
        self.assertTrue(response.queryTime > 0.0001, response.asJson())

    def testJoin(self):
        luceneB = Lucene(join(self.tempdir, 'luceneB'), reactor=self._reactor, commitTimeout=42, commitCount=1)
        returnValueFromGenerator(self.lucene.addDocument(identifier="id:0", document=createDocument([('A.joinid', '1'), ('field1', 'value0')]), categories=createCategories([('field1', 'first item0')])))
        returnValueFromGenerator(self.lucene.addDocument(identifier="id:1", document=createDocument([('A.joinid', '2'), ('field1', 'value0')]), categories=createCategories([('field1', 'first item1')])))
        returnValueFromGenerator(luceneB.addDocument(identifier="id:2", document=createDocument([('B.joinid', '1'), ('field2', 'value1')]), categories=createCategories([('field2', 'first item2')])))
        returnValueFromGenerator(luceneB.addDocument(identifier="id:3", document=createDocument([('B.joinid', '2'), ('field3', 'value3')]), categories=createCategories([('field2', 'first item3')])))

        sleep(0.5)

        joinFilter = luceneB.createJoinFilter(TermQuery(Term("field2", "value1")), fromField='B.joinid', toField='A.joinid')
        joinCollector = TermsCollector.create('A.joinid', False)
        response = returnValueFromGenerator(self.lucene.executeQuery(TermQuery(Term('field1', 'value0')), facets=[dict(maxTerms=10, fieldname='field1')], collectors={'joinfacet': joinCollector}, filters=[joinFilter]))
        joinDrilldownData = luceneB.joinFacet(termsCollector=joinCollector, fromField='B.joinid', facets=[dict(maxTerms=10, fieldname='field2')])
        response.drilldownData.extend(joinDrilldownData)

        self.assertEquals(1, response.total)
        self.assertEquals(['id:0'], response.hits)
        self.assertEquals([{'terms': [{'count': 1, 'term': u'first item0'}], 'fieldname': u'field1'}, {'terms': [{'count': 1, 'term': u'first item2'}], 'fieldname': u'field2'}], response.drilldownData)

    def testSuggestions(self):
        returnValueFromGenerator(self.lucene.addDocument(identifier="id:0", document=createDocument([('field1', 'value0'), ('field2', 'value2'), ('field3', 'value2')])))
        sleep(0.1)
        response = returnValueFromGenerator(self.lucene.executeQuery(luceneQuery=MatchAllDocsQuery(), suggestionRequest=dict(count=2, query="value0 and valeu", field="field3")))
        self.assertEquals(['id:0'], response.hits)
        self.assertEquals({'value0': (0, 6, ['value2']), 'valeu': (11, 16, ['value2'])}, response.suggestions)

    def testRangeQuery(self):
        for f in ['aap', 'noot', 'mies', 'vis', 'vuur', 'boom']:
            returnValueFromGenerator(self.lucene.addDocument(identifier="id:%s" % f, document=createDocument([('field', f)])))
        sleep(0.1)
        # (field, lowerTerm, upperTerm, includeLower, includeUpper)
        luceneQuery = TermRangeQuery.newStringRange('field', None, 'mies', False, False) # <
        response = returnValueFromGenerator(self.lucene.executeQuery(luceneQuery=luceneQuery))
        self.assertEquals(set(['id:aap', 'id:boom']), set(response.hits))
        luceneQuery = TermRangeQuery.newStringRange('field', None, 'mies', False, True) # <=
        response = returnValueFromGenerator(self.lucene.executeQuery(luceneQuery=luceneQuery))
        self.assertEquals(set(['id:aap', 'id:boom', 'id:mies']), set(response.hits))
        luceneQuery = TermRangeQuery.newStringRange('field', 'mies', None, False, True) # >
        response = returnValueFromGenerator(self.lucene.executeQuery(luceneQuery=luceneQuery))
        self.assertEquals(set(['id:noot', 'id:vis', 'id:vuur']), set(response.hits))
        luceneQuery = LuceneQueryComposer([]).compose(parseCql('field >= mies'))
        response = returnValueFromGenerator(self.lucene.executeQuery(luceneQuery=luceneQuery))
        self.assertEquals(set(['id:mies', 'id:noot', 'id:vis', 'id:vuur']), set(response.hits))

    def testFieldnames(self):
        returnValueFromGenerator(self.lucene.addDocument(identifier="id:0", document=createDocument([('field0', 'value0')])))
        returnValueFromGenerator(self.lucene.addDocument(identifier="id:1", document=createDocument([('field1', 'value0')])))
        returnValueFromGenerator(self.lucene.addDocument(identifier="id:2", document=createDocument([('field1', 'value0')])))
        sleep(0.1)
        response = returnValueFromGenerator(self.lucene.fieldnames())
        self.assertEquals(set([IDFIELD, 'field0', 'field1']), set(response.hits))
        self.assertEquals(3, response.total)


def createDocument(textfields):
    document = Document()
    for name, value in textfields:
        document.add(TextField(name, value, Field.Store.NO))
    return document

def createCategories(fields):
    result = []
    for name, value in fields:
        result.append(CategoryPath([name, str(value)]))
    return result

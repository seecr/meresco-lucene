# -*- encoding: utf-8 -*-
## begin license ##
#
# "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
#
# Copyright (C) 2013-2015 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2013-2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
# Copyright (C) 2015 Koninklijke Bibliotheek (KB) http://www.kb.nl
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


from os.path import join
import gc

from cqlparser import parseString as parseCql

from weightless.core import consume, retval

from meresco.lucene import Lucene, VM, DrilldownField, LuceneSettings
from meresco.lucene._lucene import IDFIELD
from meresco.lucene.hit import Hit
from meresco.lucene.fieldregistry import NO_TERMS_FREQUENCY_FIELDTYPE, FieldRegistry, INTFIELD
from meresco.lucene.lucenequerycomposer import LuceneQueryComposer

from org.apache.lucene.search import MatchAllDocsQuery, TermQuery, TermRangeQuery, BooleanQuery, BooleanClause, PhraseQuery
from org.apache.lucene.document import Document, TextField, Field, NumericDocValuesField, StringField
from org.apache.lucene.index import Term
from org.apache.lucene.facet import FacetField
from org.meresco.lucene.analysis import MerescoDutchStemmingAnalyzer
from org.meresco.lucene.search import MerescoClusterer

from seecr.test import CallTrace
from seecr.test.io import stdout_replaced

from lucenetestcase import LuceneTestCase

class LuceneTest(LuceneTestCase):
    def setUp(self):
        super(LuceneTest, self).setUp(FIELD_REGISTRY)

    def hitIds(self, hits):
        return [hit.id for hit in hits]

    def testCreate(self):
        result = retval(self.lucene.executeQuery(MatchAllDocsQuery()))
        self.assertEquals(0, result.total)
        self.assertTrue(hasattr(result, 'times'))

    def testAdd1Document(self):
        document = Document()
        document.add(TextField('title', 'The title', Field.Store.NO))
        retval(self.lucene.addDocument(identifier="identifier", document=document))
        result = retval(self.lucene.executeQuery(MatchAllDocsQuery()))
        self.assertEquals(1, result.total)
        self.assertEquals(['identifier'], self.hitIds(result.hits))
        result = retval(self.lucene.executeQuery(TermQuery(Term("title", 'title'))))
        self.assertEquals(1, result.total)
        result = retval(self.lucene.executeQuery(TermQuery(Term("title", 'the'))))
        self.assertEquals(1, result.total)
        self.assertTrue(result.queryTime > 0.0001, result.asJson())
        self.assertEquals({'query': {
                'drilldownQueries': None,
                'facets': None,
                'filterQueries': None,
                'luceneQuery': 'title:the',
                'start': 0,
                'stop': 10,
                'suggestionRequest': None
            },
            'type': 'Query'}, result.info)

    def testAdd1DocumentWithReadonlyLucene(self):
        settings = LuceneSettings(commitTimeout=1, verbose=False, readonly=True)
        readOnlyLucene = Lucene(
            join(self.tempdir, 'lucene'),
            reactor=self._reactor,
            settings=settings,
        )
        self.assertEquals(['addTimer'], self._reactor.calledMethodNames())
        timer = self._reactor.calledMethods[0]
        document = Document()
        document.add(TextField('title', 'The title', Field.Store.NO))
        retval(self.lucene.addDocument(identifier="identifier", document=document))
        result = retval(readOnlyLucene.executeQuery(MatchAllDocsQuery()))
        self.assertEquals(0, result.total)
        timer.kwargs['callback']()
        result = retval(readOnlyLucene.executeQuery(MatchAllDocsQuery()))
        self.assertEquals(1, result.total)

        readOnlyLucene.close()
        readOnlyLucene = None

    def testAddAndDeleteDocument(self):
        retval(self.lucene.addDocument(identifier="id:0", document=Document()))
        retval(self.lucene.addDocument(identifier="id:1", document=Document()))
        retval(self.lucene.addDocument(identifier="id:2", document=Document()))
        retval(self.lucene.delete(identifier="id:1"))
        result = retval(self.lucene.executeQuery(MatchAllDocsQuery()))
        self.assertEquals(2, result.total)
        self.assertEquals(set(['id:0', 'id:2']), set(self.hitIds(result.hits)))

    def testAddDocumentWithoutIdentifier(self):
        retval(self.lucene.addDocument(document=Document()))
        retval(self.lucene.addDocument(document=Document()))
        result = retval(self.lucene.executeQuery(MatchAllDocsQuery()))
        self.assertEquals(2, result.total)
        self.assertEquals([None, None], self.hitIds(result.hits))

    def testAddCommitAfterTimeout(self):
        self.lucene.close()
        self._defaultSettings.commitTimeout = 42
        self._defaultSettings.commitCount = 3
        self.lucene = Lucene(join(self.tempdir, 'lucene'), reactor=self._reactor, settings=self._defaultSettings)
        retval(self.lucene.addDocument(identifier="id:0", document=Document()))
        self.assertEquals(['addTimer'], self._reactor.calledMethodNames())
        self.assertEquals(42, self._reactor.calledMethods[0].kwargs['seconds'])
        commit = self._reactor.calledMethods[0].kwargs['callback']
        self._reactor.calledMethods.reset()
        result = retval(self.lucene.executeQuery(MatchAllDocsQuery()))
        self.assertEquals(0, result.total)
        commit()
        self.assertEquals([], self._reactor.calledMethodNames())
        result = retval(self.lucene.executeQuery(MatchAllDocsQuery()))
        self.assertEquals(1, result.total)

    def testAddAndCommitCount3(self):
        self.lucene.close()
        self._defaultSettings.commitTimeout = 42
        self._defaultSettings.commitCount = 3
        self.lucene = Lucene(join(self.tempdir, 'lucene'), reactor=self._reactor, settings=self._defaultSettings)
        token = object()
        self._reactor.returnValues['addTimer'] = token
        retval(self.lucene.addDocument(identifier="id:0", document=Document()))
        result = retval(self.lucene.executeQuery(MatchAllDocsQuery()))
        self.assertEquals(0, result.total)
        self.assertEquals(['addTimer'], self._reactor.calledMethodNames())
        self.assertEquals(42, self._reactor.calledMethods[0].kwargs['seconds'])

        retval(self.lucene.addDocument(identifier="id:1", document=Document()))
        result = retval(self.lucene.executeQuery(MatchAllDocsQuery()))
        self.assertEquals(0, result.total)
        self.assertEquals(['addTimer'], self._reactor.calledMethodNames())
        retval(self.lucene.addDocument(identifier="id:2", document=Document()))
        result = retval(self.lucene.executeQuery(MatchAllDocsQuery()))
        self.assertEquals(3, result.total)
        self.assertEquals(['addTimer', 'removeTimer'], self._reactor.calledMethodNames())
        self.assertEquals(token, self._reactor.calledMethods[1].kwargs['token'])

    def testAddTwiceUpdatesDocument(self):
        retval(self.lucene.addDocument(identifier="id:0", document=createDocument([
                ('field0', 'value0'),
                ('field1', 'value1'),
            ])))
        retval(self.lucene.addDocument(identifier="id:0", document=createDocument([
                ('field1', 'value1'),
            ])))
        result = retval(self.lucene.executeQuery(TermQuery(Term('field1', 'value1'))))
        self.assertEquals(1, result.total)
        result = retval(self.lucene.executeQuery(TermQuery(Term('field0', 'value0'))))
        self.assertEquals(0, result.total)

    def testSorting(self):
        retval(self.lucene.addDocument(identifier="id:0", document=createDocument([
                ('field0', 'AA'),
                ('field1', 'ZZ'),
                ('field2', 'AA'),
                ('field3', 'X'),
            ])))
        retval(self.lucene.addDocument(identifier="id:1", document=createDocument([
                ('field0', 'BB'),
                ('field1', 'AA'),
                ('field2', 'ZZ'),
                ('field3', 'X X'),
            ])))
        retval(self.lucene.addDocument(identifier="id:2", document=createDocument([
                ('field0', 'CC'),
                ('field1', 'ZZ'),
                ('field3', 'X X X'),
            ])))
        result = retval(self.lucene.executeQuery(MatchAllDocsQuery(), sortKeys=[dict(sortBy='field0', sortDescending=False)]))
        self.assertEquals(3, result.total)
        self.assertEquals(['id:0', 'id:1', 'id:2'], self.hitIds(result.hits))
        result = retval(self.lucene.executeQuery(MatchAllDocsQuery(), sortKeys=[dict(sortBy='field0', sortDescending=True)]))
        self.assertEquals(['id:2', 'id:1', 'id:0'], self.hitIds(result.hits))
        result = retval(self.lucene.executeQuery(MatchAllDocsQuery(), sortKeys=[dict(sortBy='field1', sortDescending=True), dict(sortBy='field0', sortDescending=True)]))
        self.assertEquals(['id:2', 'id:0', 'id:1'], self.hitIds(result.hits))
        result = retval(self.lucene.executeQuery(MatchAllDocsQuery(), sortKeys=[dict(sortBy='field2', sortDescending=True)]))
        self.assertEquals(['id:1', 'id:0', 'id:2'], self.hitIds(result.hits))
        result = retval(self.lucene.executeQuery(MatchAllDocsQuery(), sortKeys=[dict(sortBy='field2', sortDescending=False)]))
        self.assertEquals(['id:0', 'id:1', 'id:2'], self.hitIds(result.hits))

        result = retval(self.lucene.executeQuery(TermQuery(Term('field3', 'x')), sortKeys=[dict(sortBy='score', sortDescending=True), dict(sortBy='field1', sortDescending=True)]))
        self.assertEquals(['id:2', 'id:1', 'id:0'], self.hitIds(result.hits))


    def testStartStop(self):
        retval(self.lucene.addDocument(identifier="id:0", document=createDocument([('field1', 'ishallnotbetokenizedA')])))
        retval(self.lucene.addDocument(identifier="id:1", document=createDocument([('field1', 'ishallnotbetokenizedB')])))
        retval(self.lucene.addDocument(identifier="id:2", document=createDocument([('field1', 'ishallnotbetokenizedC')])))
        result = retval(self.lucene.executeQuery(MatchAllDocsQuery(), start=1, stop=10, sortKeys=[dict(sortBy='field1', sortDescending=False)]))
        self.assertEquals(3, result.total)
        self.assertEquals(['id:1', 'id:2'], self.hitIds(result.hits))
        result = retval(self.lucene.executeQuery(MatchAllDocsQuery(), start=0, stop=2, sortKeys=[dict(sortBy='field1', sortDescending=False)]))
        self.assertEquals(['id:0', 'id:1'], self.hitIds(result.hits))
        result = retval(self.lucene.executeQuery(MatchAllDocsQuery(), start=0, stop=0, sortKeys=[dict(sortBy='field1', sortDescending=False)]))
        self.assertEquals(3, result.total)
        self.assertEquals([], self.hitIds(result.hits))
        result = retval(self.lucene.executeQuery(MatchAllDocsQuery(), start=2, stop=2, sortKeys=[dict(sortBy='field1', sortDescending=False)]))
        self.assertEquals(3, result.total)
        self.assertEquals([], self.hitIds(result.hits))

        result = retval(self.lucene.executeQuery(MatchAllDocsQuery(), start=1, stop=2, sortKeys=[dict(sortBy='field1', sortDescending=False)]))
        self.assertEquals(3, result.total)
        self.assertEquals(['id:1'], self.hitIds(result.hits))

    def testFacets(self):
        retval(self.lucene.addDocument(identifier="id:0", document=createDocument([('field1', 'id:0')], facets=[('field2', 'first item0'), ('field3', 'second item')])))
        retval(self.lucene.addDocument(identifier="id:1", document=createDocument([('field1', 'id:1')], facets=[('field2', 'first item1'), ('field3', 'other value')])))
        retval(self.lucene.addDocument(identifier="id:2", document=createDocument([('field1', 'id:2')], facets=[('field2', 'first item2'), ('field3', 'second item')])))

        self.assertEquals(set([u'$facets', u'__id__', u'field1']), set(self.lucene._index.fieldnames()))

        # does not crash!!!
        retval(self.lucene.executeQuery(MatchAllDocsQuery(), facets=[dict(maxTerms=10, fieldname='field2')]))
        result = retval(self.lucene.executeQuery(MatchAllDocsQuery()))
        self.assertEquals([], result.drilldownData)
        result = retval(self.lucene.executeQuery(MatchAllDocsQuery(), facets=[dict(maxTerms=10, fieldname='field2')]))

        self.assertEquals([{
                'fieldname': 'field2',
                'path': [],
                'terms': [
                    {'term': 'first item0', 'count': 1},
                    {'term': 'first item1', 'count': 1},
                    {'term': 'first item2', 'count': 1},
                ],
            }],result.drilldownData)
        result = retval(self.lucene.executeQuery(MatchAllDocsQuery(), facets=[dict(maxTerms=10, fieldname='field3')]))
        self.assertEquals([{
                'fieldname': 'field3',
                'path': [],
                'terms': [
                    {'term': 'second item', 'count': 2},
                    {'term': 'other value', 'count': 1},
                ],
            }],result.drilldownData)

    def testFacetsInMultipleFields(self):
        retval(self.lucene.addDocument(identifier="id:0", document=createDocument([('field1', 'id:0')], facets=[('field2', 'first item0'), ('field3', 'second item')])))
        retval(self.lucene.addDocument(identifier="id:1", document=createDocument([('field1', 'id:1')], facets=[('field2', 'first item1'), ('field3', 'other value')])))
        retval(self.lucene.addDocument(identifier="id:2", document=createDocument([('field1', 'id:2')], facets=[('field2', 'first item2'), ('field3', 'second item')])))
        retval(self.lucene.addDocument(identifier="id:2", document=createDocument([('field1', 'id:2')], facets=[('field_other', 'first item1'), ('field3', 'second item')])))

        self.assertEquals(set([u'$facets', u'other', u'__id__', u'field1']), set(self.lucene._index.fieldnames()))

    def testFacetsWithUnsupportedSortBy(self):
        try:
            retval(self.lucene.executeQuery(MatchAllDocsQuery(), facets=[dict(maxTerms=10, fieldname='field2', sortBy='incorrectSort')]))
        except ValueError, e:
            self.assertEquals("""Value of "sortBy" should be in ['count']""", str(e))

    def testFacetsOnUnknownField(self):
        result = retval(self.lucene.executeQuery(MatchAllDocsQuery(), facets=[dict(maxTerms=10, fieldname='fieldUnknonw')]))
        self.assertEquals([{'terms': [], 'path': [], 'fieldname': 'fieldUnknonw'}], result.drilldownData)

    def testFacetsMaxTerms0(self):
        self.lucene._index._commitCount = 3
        retval(self.lucene.addDocument(identifier="id:0", document=createDocument([('field1', 'id:0')], facets=[('field2', 'first item0'), ('field3', 'second item')])))
        retval(self.lucene.addDocument(identifier="id:1", document=createDocument([('field1', 'id:1')], facets=[('field2', 'first item1'), ('field3', 'other value')])))
        retval(self.lucene.addDocument(identifier="id:2", document=createDocument([('field1', 'id:2')], facets=[('field2', 'first item2'), ('field3', 'second item')])))

        result = retval(self.lucene.executeQuery(MatchAllDocsQuery(), facets=[dict(maxTerms=0, fieldname='field3')]))
        self.assertEquals([{
                'fieldname': 'field3',
                'path': [],
                'terms': [
                    {'term': 'second item', 'count': 2},
                    {'term': 'other value', 'count': 1},
                ],
            }],result.drilldownData)

    def testFacetsWithCategoryPathHierarchy(self):
        retval(self.lucene.addDocument(identifier="id:0", document=createDocument([('field1', 'id:0')], facets=[('fieldHier', ['item0', 'item1'])])))
        retval(self.lucene.addDocument(identifier="id:1", document=createDocument([('field1', 'id:1')], facets=[('fieldHier', ['item0', 'item2'])])))
        retval(self.lucene.addDocument(identifier="id:2", document=createDocument([('field1', 'id:2')], facets=[('fieldHier', ['item3', 'item4'])])))
        result = retval(self.lucene.executeQuery(MatchAllDocsQuery(), facets=[dict(maxTerms=10, path=[], fieldname='fieldHier')]))
        self.assertEquals([{
                'fieldname': 'fieldHier',
                'path': [],
                'terms': [
                    {
                        'term': 'item0',
                        'count': 2,
                        'subterms': [
                            {'term': 'item1', 'count': 1},
                            {'term': 'item2', 'count': 1},
                        ]
                    },
                    {
                        'term': 'item3',
                        'count': 1,
                        'subterms': [
                            {'term': 'item4', 'count': 1},
                        ]
                    }
                ],
            }], result.drilldownData)

        result = retval(self.lucene.executeQuery(MatchAllDocsQuery(), facets=[dict(maxTerms=10, fieldname='fieldHier', path=['item0'])]))
        self.assertEquals([{
                'fieldname': 'fieldHier',
                'path': ['item0'],
                'terms': [
                    {'term': 'item1', 'count': 1},
                    {'term': 'item2', 'count': 1},
                ],
            }], result.drilldownData)

    def XX_testFacetsWithIllegalCharacters(self):
        categories = createCategories([('field', 'a/b')])
        # The following print statement causes an error to be printed to stderr.
        # It keeps on working.
        self.assertEquals('[<CategoryPath: class org.apache.lucene.facet.taxonomy.CategoryPath>]', str(categories))
        retval(self.lucene.addDocument(identifier="id:0", document=createDocument([]), categories=categories))
        result = retval(self.lucene.executeQuery(MatchAllDocsQuery(), facets=[dict(maxTerms=10, fieldname='field')]))
        self.assertEquals([{
                'fieldname': 'field',
                'terms': [
                    {'term': 'a/b', 'count': 1},
                ],
            }],result.drilldownData)

    def testEscapeFacets(self):
        retval(self.lucene.addDocument(identifier="id:0", document=createDocument([('field1', 'id:0')], facets=[('field2', 'first/item0')])))
        result = retval(self.lucene.executeQuery(MatchAllDocsQuery(), facets=[dict(maxTerms=10, fieldname='field2')]))
        self.assertEquals([{
                'terms': [{'count': 1, 'term': u'first/item0'}],
                'path': [],
                'fieldname': u'field2'
            }],result.drilldownData)

    def testDiacritics(self):
        retval(self.lucene.addDocument(identifier='hendrik', document=createDocument([('title', 'Waar is Morée vandaag?')])))
        result = retval(self.lucene.executeQuery(TermQuery(Term('title', 'moree'))))
        self.assertEquals(1, result.total)
        query = LuceneQueryComposer(unqualifiedTermFields=[], luceneSettings=LuceneSettings()).compose(parseCql("title=morée"))
        result = retval(self.lucene.executeQuery(query))
        self.assertEquals(1, result.total)

    def testFilterQueries(self):
        for i in xrange(10):
            retval(self.lucene.addDocument(identifier="id:%s" % i, document=createDocument([
                    ('mod2', 'v%s' % (i % 2)),
                    ('mod3', 'v%s' % (i % 3))
                ])))
        # id     0  1  2  3  4  5  6  7  8  9
        # mod2  v0 v1 v0 v1 v0 v1 v0 v1 v0 v1
        # mod3  v0 v1 v2 v0 v1 v2 v0 v1 v2 v0
        result = retval(self.lucene.executeQuery(MatchAllDocsQuery(), filterQueries=[TermQuery(Term('mod2', 'v0'))]))
        self.assertEquals(5, result.total)
        self.assertEquals(set(['id:0', 'id:2', 'id:4', 'id:6', 'id:8']), set(self.hitIds(result.hits)))
        result = retval(self.lucene.executeQuery(MatchAllDocsQuery(), filterQueries=[
                TermQuery(Term('mod2', 'v0')),
                TermQuery(Term('mod3', 'v0')),
            ]))
        self.assertEquals(2, result.total)
        self.assertEquals(set(['id:0', 'id:6']), set(self.hitIds(result.hits)))

    def testPrefixSearch(self):
        response = retval(self.lucene.prefixSearch(fieldname='field1', prefix='valu'))
        self.assertEquals([], response.hits)
        retval(self.lucene.addDocument(identifier="id:0", document=createDocument([('field1', 'value0')])))
        retval(self.lucene.addDocument(identifier="id:1", document=createDocument([('field1', 'value1')])))
        retval(self.lucene.addDocument(identifier="id:2", document=createDocument([('field1', 'value1')])))
        response = retval(self.lucene.prefixSearch(fieldname='field1', prefix='valu'))
        self.assertEquals(['value1', 'value0'], response.hits)
        self.assertTrue(response.queryTime > 0, response.asJson())

    def testPrefixSearchForIntField(self):
        retval(self.lucene.addDocument(identifier='id:0', document=createDocument([('intField', 1)])))
        for i in xrange(5):
            retval(self.lucene.addDocument(identifier='id:%s' % (i+20), document=createDocument([('intField', i+20)])))
        response = retval(self.lucene.prefixSearch(fieldname='intField', prefix=None))
        self.assertEquals([0, 0, 0, 24, 23, 22, 21, 20, 1], response.hits) # No fix for the 0's yet

    def testSuggestions(self):
        retval(self.lucene.addDocument(identifier="id:0", document=createDocument([('field1', 'value0'), ('field2', 'value2'), ('field3', 'value2')])))
        response = retval(self.lucene.executeQuery(luceneQuery=MatchAllDocsQuery(), suggestionRequest=dict(count=2, query="value0 and valeu", field="field3")))
        self.assertEquals(['id:0'], self.hitIds(response.hits))
        self.assertEquals({'value0': (0, 6, ['value2']), 'valeu': (11, 16, ['value2'])}, response.suggestions)

    def testRangeQuery(self):
        for f in ['aap', 'noot', 'mies', 'vis', 'vuur', 'boom']:
            retval(self.lucene.addDocument(identifier="id:%s" % f, document=createDocument([('field', f)])))
        # (field, lowerTerm, upperTerm, includeLower, includeUpper)
        luceneQuery = TermRangeQuery.newStringRange('field', None, 'mies', False, False) # <
        response = retval(self.lucene.executeQuery(luceneQuery=luceneQuery))
        self.assertEquals(set(['id:aap', 'id:boom']), set(self.hitIds(response.hits)))
        luceneQuery = TermRangeQuery.newStringRange('field', None, 'mies', False, True) # <=
        response = retval(self.lucene.executeQuery(luceneQuery=luceneQuery))
        self.assertEquals(set(['id:aap', 'id:boom', 'id:mies']), set(self.hitIds(response.hits)))
        luceneQuery = TermRangeQuery.newStringRange('field', 'mies', None, False, True) # >
        response = retval(self.lucene.executeQuery(luceneQuery=luceneQuery))
        self.assertEquals(set(['id:noot', 'id:vis', 'id:vuur']), set(self.hitIds(response.hits)))
        luceneQuery = LuceneQueryComposer([], luceneSettings=LuceneSettings()).compose(parseCql('field >= mies'))
        response = retval(self.lucene.executeQuery(luceneQuery=luceneQuery))
        self.assertEquals(set(['id:mies', 'id:noot', 'id:vis', 'id:vuur']), set(self.hitIds(response.hits)))

    def testFieldnames(self):
        retval(self.lucene.addDocument(identifier="id:0", document=createDocument([('field0', 'value0')])))
        retval(self.lucene.addDocument(identifier="id:1", document=createDocument([('field1', 'value0')])))
        retval(self.lucene.addDocument(identifier="id:2", document=createDocument([('field1', 'value0')])))
        response = retval(self.lucene.fieldnames())
        self.assertEquals(set([IDFIELD, 'field0', 'field1']), set(response.hits))
        self.assertEquals(3, response.total)

    def testDrilldownFieldnames(self):
        retval(self.lucene.addDocument(identifier="id:0", document=createDocument([('field0', 'value0')], facets=[("cat", "cat-A"), ("cat", "cat-B")])))
        retval(self.lucene.addDocument(identifier="id:1", document=createDocument([('field1', 'value0')], facets=[("cat", "cat-A"), ("cat2", "cat-B")])))
        retval(self.lucene.addDocument(identifier="id:2", document=createDocument([('field1', 'value0')], facets=[("cat2", "cat-A"), ("cat3", "cat-B")])))
        response = retval(self.lucene.drilldownFieldnames())
        self.assertEquals(set(['cat', 'cat2', 'cat3']), set(response.hits))
        self.assertEquals(3, response.total)
        response = retval(self.lucene.drilldownFieldnames(['cat']))
        self.assertEquals(set(['cat-A', 'cat-B']), set(response.hits))

    def testFilterCaching(self):
        for i in range(10):
            retval(self.lucene.addDocument(identifier="id:%s" % i, document=createDocument([('field%s' % i, 'value0')])))
        query = BooleanQuery()
        [query.add(TermQuery(Term("field%s" % i, "value0")), BooleanClause.Occur.SHOULD) for i in range(100)]
        response = retval(self.lucene.executeQuery(luceneQuery=MatchAllDocsQuery(), filterQueries=[query]))
        responseWithCaching = retval(self.lucene.executeQuery(luceneQuery=MatchAllDocsQuery(), filterQueries=[query]))
        self.assertTrue(responseWithCaching.queryTime < response.queryTime)

    def testHandleShutdown(self):
        document = Document()
        document.add(TextField('title', 'The title', Field.Store.NO))
        retval(self.lucene.addDocument(identifier="identifier", document=document))
        with stdout_replaced():
            self.lucene.handleShutdown()
        lucene = Lucene(join(self.tempdir, 'lucene'), reactor=self._reactor, settings=self._defaultSettings)
        response = retval(lucene.executeQuery(luceneQuery=MatchAllDocsQuery()))
        self.assertEquals(1, response.total)

    def testResultsFilterCollector(self):
        doc = document(field0='v0')
        doc.add(NumericDocValuesField("__key__", long(42)))
        doc.add(FacetField("cat", ["cat-A"]))
        consume(self.lucene.addDocument(identifier="urn:1", document=doc))
        doc = document(field0='v1')
        doc.add(NumericDocValuesField("__key__", long(42)))
        doc.add(FacetField("cat", ["cat-A"]))
        consume(self.lucene.addDocument(identifier="urn:2", document=doc))
        self.lucene.commit()
        result = retval(self.lucene.executeQuery(MatchAllDocsQuery(),
                        dedupField="__key__", facets=facets(cat=10)))
        self.assertEquals(1, result.total)
        hit = result.hits[0]
        #self.assertEquals('urn:1', hit.id) # this is no longer deterministic since threading
        self.assertEquals({'__key__': 2}, hit.duplicateCount)
        self.assertEquals({'count': 2, 'term': u'cat-A'}, result.drilldownData[0]['terms'][0])

    def testDedupFilterCollectorSortedByField(self):
        doc = document(field0='v0')
        doc.add(NumericDocValuesField("__key__", long(42)))
        doc.add(NumericDocValuesField("__key__.date", long(2012)))
        doc.add(FacetField("cat", ["cat-A"]))
        consume(self.lucene.addDocument(identifier="urn:1", document=doc))

        doc = document(field0='v1')
        doc.add(NumericDocValuesField("__key__", long(42)))
        doc.add(NumericDocValuesField("__key__.date", long(2013))) # first hit of 3 duplicates
        doc.add(FacetField("cat", ["cat-A"]))
        consume(self.lucene.addDocument(identifier="urn:2", document=doc))

        doc = document(field0='v2')
        doc.add(NumericDocValuesField("__key__", long(42)))
        doc.add(FacetField("cat", ["cat-A"]))
        consume(self.lucene.addDocument(identifier="urn:3", document=doc))

        doc = document(field0='v3')
        doc.add(FacetField("cat", ["cat-A"]))
        consume(self.lucene.addDocument(identifier="urn:4", document=doc))

        self.lucene.commit()
        result = retval(self.lucene.executeQuery(MatchAllDocsQuery(),
                        dedupField="__key__", dedupSortField='__key__.date', facets=facets(cat=10)))
        # expected two hits: "urn:2" (3x) and "urn:4" in no particular order
        self.assertEquals(2, result.total)
        self.assertEquals(4, result.totalWithDuplicates)
        expectedHits = [
            Hit(score=1.0, id=u'urn:2', duplicateCount={u'__key__': 3}),
            Hit(score=1.0, id=u'urn:4', duplicateCount={u'__key__': 1}),
        ]
        resultHits = list(hit for hit in result.hits)
        resultHits.sort(key=lambda h:h.id)
        self.assertEquals(expectedHits[0].__dict__, resultHits[0].__dict__)
        self.assertEquals(expectedHits, resultHits)
        self.assertEquals({'count': 4, 'term': u'cat-A'}, result.drilldownData[0]['terms'][0])

    def testGroupingCollector(self):
        doc = document(field0='v0')
        doc.add(NumericDocValuesField("__key__", long(42)))
        consume(self.lucene.addDocument(identifier="urn:1", document=doc))

        doc = document(field0='v1')
        doc.add(NumericDocValuesField("__key__", long(42)))
        consume(self.lucene.addDocument(identifier="urn:2", document=doc))

        doc = document(field0='v2')
        doc.add(NumericDocValuesField("__key__", long(43)))
        consume(self.lucene.addDocument(identifier="urn:3", document=doc))

        doc = document(field0='v3')
        consume(self.lucene.addDocument(identifier="urn:4", document=doc))

        self.lucene.commit()
        result = retval(self.lucene.executeQuery(MatchAllDocsQuery(), groupingField="__key__", stop=3))
        # expected two hits: "urn:2" (3x) and "urn:4" in no particular order
        self.assertEquals(4, result.total)
        expectedHits = [
            Hit(score=1.0, id=u'urn:1', duplicates={u'__key__': [{'id': 'urn:1'}, {'id': 'urn:2'}]}),
            Hit(score=1.0, id=u'urn:3', duplicates={u'__key__': [{'id': 'urn:3'}]}),
            Hit(score=1.0, id=u'urn:4', duplicates={u'__key__': [{'id': 'urn:4'}]}),
        ]
        resultHits = list(hit for hit in result.hits)
        resultHits.sort(key=lambda h:h.id)
        for hit in resultHits:
            hit.duplicates['__key__'].sort()
        self.assertEquals(expectedHits[0].__dict__, resultHits[0].__dict__)
        self.assertEquals(expectedHits, resultHits)

    def testGroupingOnNonExistingFieldCollector(self):
        doc = document(field0='v0')
        doc.add(NumericDocValuesField("__key__", long(42)))
        consume(self.lucene.addDocument(identifier="urn:1", document=doc))

        doc = document(field0='v1')
        doc.add(NumericDocValuesField("__key__", long(42)))
        consume(self.lucene.addDocument(identifier="urn:2", document=doc))

        doc = document(field0='v2')
        doc.add(NumericDocValuesField("__key__", long(43)))
        consume(self.lucene.addDocument(identifier="urn:3", document=doc))

        doc = document(field0='v3')
        consume(self.lucene.addDocument(identifier="urn:4", document=doc))

        self.lucene.commit()
        result = retval(self.lucene.executeQuery(MatchAllDocsQuery(), groupingField="__other_key__", stop=3))
        # expected two hits: "urn:2" (3x) and "urn:4" in no particular order
        self.assertEquals(4, result.total)
        self.assertEquals(3, len(result.hits))

    def testDontGroupIfMaxResultsAreLessThanTotalRecords(self):
        doc = document(field0='v0')
        doc.add(NumericDocValuesField("__key__", long(42)))
        consume(self.lucene.addDocument(identifier="urn:1", document=doc))

        doc = document(field0='v1')
        doc.add(NumericDocValuesField("__key__", long(42)))
        consume(self.lucene.addDocument(identifier="urn:2", document=doc))

        self.lucene.commit()
        result = retval(self.lucene.executeQuery(MatchAllDocsQuery(), groupingField="__key__", stop=10))
        # expected two hits: "urn:2" (3x) and "urn:4" in no particular order
        self.assertEquals(2, result.total)
        expectedHits = [
            Hit(score=1.0, id=u'urn:1', duplicates={u'__key__': [{'id': 'urn:1'}]}),
            Hit(score=1.0, id=u'urn:2', duplicates={u'__key__': [{'id': 'urn:2'}]}),
        ]
        resultHits = list(hit for hit in result.hits)
        resultHits.sort(key=lambda h:h.id)
        self.assertEquals(expectedHits, resultHits)

    def testGroupingCollectorReturnsMaxHitAfterGrouping(self):
        doc = document(field0='v0')
        doc.add(NumericDocValuesField("__key__", long(42)))
        consume(self.lucene.addDocument(identifier="urn:1", document=doc))
        doc = document(field0='v0')
        doc.add(NumericDocValuesField("__key__", long(42)))
        consume(self.lucene.addDocument(identifier="urn:2", document=doc))
        for i in range(3, 11):
            doc = document(field0='v0')
            consume(self.lucene.addDocument(identifier="urn:%s" % i, document=doc))
        self.lucene.commit()
        result = retval(self.lucene.executeQuery(MatchAllDocsQuery(), groupingField="__key__", stop=5))
        self.assertEquals(10, result.total)
        self.assertEquals(5, len(result.hits))

    def testDutchStemming(self):
        self.lucene.close()
        settings = LuceneSettings(commitCount=1, analyzer=MerescoDutchStemmingAnalyzer(), verbose=False)
        self.lucene = Lucene(join(self.tempdir, 'lucene'), reactor=self._reactor, settings=settings)
        doc = document(field0='katten en honden')
        consume(self.lucene.addDocument(identifier="urn:1", document=doc))
        self.lucene.commit()

        result = retval(self.lucene.executeQuery(TermQuery(Term("field0", 'katten'))))
        self.assertEquals(1, result.total)

        result = retval(self.lucene.executeQuery(TermQuery(Term("field0", 'kat'))))
        self.assertEquals(1, result.total)

        q = self.createPhraseQuery("field0", "katten", "en", "honden")
        result = retval(self.lucene.executeQuery(q))
        self.assertEquals(1, result.total)

        q = self.createPhraseQuery("field0", "kat", "en", "honden")
        result = retval(self.lucene.executeQuery(q))
        self.assertEquals(1, result.total)

        q = self.createPhraseQuery("field0", "katten", "en", "hond")
        result = retval(self.lucene.executeQuery(q))
        self.assertEquals(1, result.total)

        q = self.createPhraseQuery("field0", "kat", "en", "hond")
        result = retval(self.lucene.executeQuery(q))
        self.assertEquals(1, result.total)

    def createPhraseQuery(self, field, *args):
        q = PhraseQuery()
        for term in args:
            q.add(Term(field, term))
        return q

    def testDrilldownQuery(self):
        doc = createDocument(fields=[("field0", 'v1')], facets=[("cat", "cat-A")])
        consume(self.lucene.addDocument(identifier="urn:1", document=doc))
        doc = createDocument(fields=[("field0", 'v2')], facets=[("cat", "cat-B")])
        consume(self.lucene.addDocument(identifier="urn:2", document=doc))

        result = retval(self.lucene.executeQuery(MatchAllDocsQuery()))
        self.assertEquals(2, result.total)

        result = retval(self.lucene.executeQuery(MatchAllDocsQuery(), drilldownQueries=[("cat", ["cat-A"])]))
        self.assertEquals(1, result.total)

        result = retval(self.lucene.executeQuery(TermQuery(Term("field0", "v2")), drilldownQueries=[("cat", ["cat-A"])]))
        self.assertEquals(0, result.total)

    def testMultipleDrilldownQueryOnSameField(self):
        doc = createDocument(fields=[("field0", 'v1')], facets=[("cat", "cat-A"), ("cat", "cat-B")])
        consume(self.lucene.addDocument(identifier="urn:1", document=doc))
        doc = createDocument(fields=[("field0", 'v1')], facets=[("cat", "cat-B",)])
        consume(self.lucene.addDocument(identifier="urn:2", document=doc))
        doc = createDocument(fields=[("field0", 'v1')], facets=[("cat", "cat-C",)])
        consume(self.lucene.addDocument(identifier="urn:3", document=doc))

        result = retval(self.lucene.executeQuery(MatchAllDocsQuery()))
        self.assertEquals(3, result.total)

        result = retval(self.lucene.executeQuery(MatchAllDocsQuery(), drilldownQueries=[("cat", ["cat-A"]), ("cat", ["cat-B"])]))
        self.assertEquals(1, result.total)

    def testNoTermFrequency(self):
        factory = FieldRegistry()
        factory.register("no.term.frequency", NO_TERMS_FREQUENCY_FIELDTYPE)
        factory.register("no.term.frequency2", NO_TERMS_FREQUENCY_FIELDTYPE)
        doc = Document()
        doc.add(factory.createField("no.term.frequency", "aap noot noot noot vuur"))
        consume(self.lucene.addDocument(identifier="no.term.frequency", document=doc))

        doc = createDocument(fields=[('term.frequency', "aap noot noot noot vuur")])
        consume(self.lucene.addDocument(identifier="term.frequency", document=doc))

        doc = Document()
        doc.add(factory.createField("no.term.frequency2", "aap noot"))
        doc.add(factory.createField("no.term.frequency2", "noot noot"))
        doc.add(factory.createField("no.term.frequency2", "vuur"))
        consume(self.lucene.addDocument(identifier="no.term.frequency2", document=doc))

        result1 = retval(self.lucene.executeQuery(TermQuery(Term("no.term.frequency", "aap"))))
        result2 = retval(self.lucene.executeQuery(TermQuery(Term("no.term.frequency", "noot"))))
        self.assertEquals(result1.hits[0].score, result2.hits[0].score)

        result1 = retval(self.lucene.executeQuery(TermQuery(Term("term.frequency", "aap"))))
        result2 = retval(self.lucene.executeQuery(TermQuery(Term("term.frequency", "noot"))))
        self.assertNotEquals(result1.hits[0].score, result2.hits[0].score)
        self.assertTrue(result1.hits[0].score < result2.hits[0].score)

        result1 = retval(self.lucene.executeQuery(TermQuery(Term("no.term.frequency2", "aap"))))
        result2 = retval(self.lucene.executeQuery(TermQuery(Term("no.term.frequency2", "noot"))))
        self.assertEquals(result1.hits[0].score, result2.hits[0].score)

        bq = BooleanQuery()
        bq.add(TermQuery(Term('no.term.frequency', 'aap')),BooleanClause.Occur.MUST)
        bq.add(TermQuery(Term('no.term.frequency', 'noot')),BooleanClause.Occur.MUST)
        self.assertEquals(1, len(retval(self.lucene.executeQuery(bq)).hits))

    def testUpdateReaderSettings(self):
        settings = self.lucene.getSettings()
        self.assertEquals({'numberOfConcurrentTasks': 6, 'similarity': u'BM25(k1=1.2,b=0.75)', 'clusterMoreRecords': 100, 'clusteringEps': 0.4, 'clusteringMinPoints': 1}, settings)

        self.lucene.setSettings(similarity=dict(k1=1.0, b=2.0), numberOfConcurrentTasks=10, clusterMoreRecords=200, clusteringEps=1.0, clusteringMinPoints=2)
        settings = self.lucene.getSettings()
        self.assertEquals({'numberOfConcurrentTasks': 10, 'similarity': u'BM25(k1=1.0,b=2.0)', 'clusterMoreRecords': 200, 'clusteringEps': 1.0, 'clusteringMinPoints': 2}, settings)

        self.lucene.setSettings(numberOfConcurrentTasks=None, similarity=None, clusterMoreRecords=None, clusteringEps=None)
        settings = self.lucene.getSettings()
        self.assertEquals({'numberOfConcurrentTasks': 6, 'similarity': u'BM25(k1=1.2,b=0.75)', 'clusterMoreRecords': 100, 'clusteringEps': 0.4, 'clusteringMinPoints': 1}, settings)

    def testClusteringOnVectors(self):
        factory = FieldRegistry(termVectorFields=['termvector'])
        for i in range(5):
            doc = Document()
            doc.add(factory.createField("termvector", "aap noot vuur %s" % i))
            consume(self.lucene.addDocument(identifier="id:%s" % i, document=doc))
        doc = Document()
        consume(self.lucene.addDocument(identifier="id:6", document=doc))
        self.lucene.commit()

        self.lucene._interpolateEpsilon = lambda *args: 0.4
        result = retval(self.lucene.executeQuery(MatchAllDocsQuery(), clusterFields=[("termvector", 1.0)]))
        self.assertEquals(2, len(result.hits))
        duplicates = [sorted([t['id'] for t in h.duplicates['topDocs']]) for h in result.hits]
        self.assertEqual(sorted([[], ['id:0', 'id:1', 'id:2', 'id:3', 'id:4']]), sorted(duplicates))

    def testInterpolateEps(self):
        self.assertEquals(0, self.lucene._interpolateEpsilon( 0, 10))
        self.assertEquals(0, self.lucene._interpolateEpsilon( 10, 10))
        self.assertEquals(0.004, self.lucene._interpolateEpsilon( 11, 10))
        self.assertEquals(0.4, self.lucene._interpolateEpsilon(110, 10))
        self.assertEquals(0.4, self.lucene._interpolateEpsilon(111, 10))

        self.assertEquals(0, self.lucene._interpolateEpsilon(0, 20))
        self.assertEquals(0, self.lucene._interpolateEpsilon(20, 20))
        self.assertEquals(0.004, self.lucene._interpolateEpsilon(21, 20))
        self.assertEquals(0.32, self.lucene._interpolateEpsilon(100, 20))
        self.assertEquals(0.4, self.lucene._interpolateEpsilon(120, 20))
        self.assertEquals(0.4, self.lucene._interpolateEpsilon(121, 20))

    def testClusteringShowOnlyRequestTop(self):
        factory = FieldRegistry(termVectorFields=['termvector'])
        for i in range(5):
            doc = Document()
            doc.add(factory.createField("termvector", "aap noot vuur %s" % i))
            consume(self.lucene.addDocument(identifier="id:%s" % i, document=doc))
        for i in range(5, 10):
            doc = Document()
            doc.add(factory.createField("termvector", "something"))
            consume(self.lucene.addDocument(identifier="id:%s" % i, document=doc))
        for i in range(10, 15):
            doc = Document()
            doc.add(factory.createField("termvector", "totally other data with more text"))
            consume(self.lucene.addDocument(identifier="id:%s" % i, document=doc))
        self.lucene.commit()

        result = retval(self.lucene.executeQuery(MatchAllDocsQuery(), clusterFields=[("termvector", 1)], start=0, stop=2))
        self.assertEquals(15, result.total)
        self.assertEquals(2, len(result.hits))

    def testClusteringRanksMostRelevantOfGroup(self):
        factory = FieldRegistry(termVectorFields=['termvector'])
        doc = Document()
        doc.add(factory.createField("termvector", "aap"))
        doc.add(factory.createField("termvector", "noot"))
        doc.add(factory.createField("termvector", "mies"))
        doc.add(factory.createField("termvector", "vuur"))
        consume(self.lucene.addDocument(identifier="id:1", document=doc))

        doc = Document()
        doc.add(factory.createField("termvector", "aap"))
        doc.add(factory.createField("termvector", "mies"))
        doc.add(factory.createField("termvector", "vuur"))
        consume(self.lucene.addDocument(identifier="id:2", document=doc))

        doc = Document()
        doc.add(factory.createField("termvector", "aap"))
        doc.add(factory.createField("termvector", "noot"))
        consume(self.lucene.addDocument(identifier="id:3", document=doc))

        self.lucene.setSettings(clusteringEps=10.0)
        self.lucene._interpolateEpsilon = lambda *args: 10.0
        result = retval(self.lucene.executeQuery(MatchAllDocsQuery(), clusterFields=[("termvector", 1)]))
        self.assertEquals(3, result.total)
        self.assertEquals(1, len(result.hits))
        self.assertEqual('id:1', result.hits[0].id)
        self.assertEqual(['id:1', 'id:2', 'id:3'], [t['id'] for t in result.hits[0].duplicates['topDocs']])
        self.assertEqual(['aap', 'noot', 'mies', 'vuur'], [t['term'] for t in result.hits[0].duplicates['topTerms']])

    def testClusteringWinsOverGroupingAndDedup(self):
        factory = FieldRegistry(termVectorFields=['termvector'])
        for i in range(15):
            doc = Document()
            doc.add(factory.createField("termvector", "aap noot vuur"))
            consume(self.lucene.addDocument(identifier="id:%s" % i, document=doc))
        doc = Document()
        doc.add(factory.createField("termvector", "something else"))
        consume(self.lucene.addDocument(identifier="id:95", document=doc))
        doc = Document()
        doc.add(factory.createField("termvector", "totally other data with more text"))
        consume(self.lucene.addDocument(identifier="id:96", document=doc))
        doc = Document()
        doc.add(factory.createField("termvector", "this is again a record"))
        consume(self.lucene.addDocument(identifier="id:97", document=doc))
        doc = Document()
        doc.add(factory.createField("termvector", "and this is also just something"))
        consume(self.lucene.addDocument(identifier="id:98", document=doc))
        self.lucene.commit()

        result = retval(self.lucene.executeQuery(MatchAllDocsQuery(), dedupField="dedupField", clusterFields=[("termvector", 1)], start=0, stop=5))
        self.assertEquals(5, len(result.hits))
        self.assertTrue(hasattr(result.hits[0], "duplicates"))

    def testClusterOnMultipleFields(self):
        factory = FieldRegistry(termVectorFields=['termvector1', 'termvector2'])
        for i in range(15):
            doc = Document()
            doc.add(factory.createField("termvector1", "aap noot vuur"))
            consume(self.lucene.addDocument(identifier="id:%s" % i, document=doc))

        doc = Document()
        doc.add(factory.createField("termvector1", "aap noot vuur"))
        doc.add(factory.createField("termvector2", "mies water"))
        consume(self.lucene.addDocument(identifier="id:100", document=doc))

        doc = Document()
        doc.add(factory.createField("termvector1", "aap vuur"))
        doc.add(factory.createField("termvector2", "mies"))
        consume(self.lucene.addDocument(identifier="id:200", document=doc))

        doc = Document()
        doc.add(factory.createField("termvector2", "iets"))
        consume(self.lucene.addDocument(identifier="id:300", document=doc))

        doc = Document()
        consume(self.lucene.addDocument(identifier="id:400", document=doc))

        result = retval(self.lucene.executeQuery(MatchAllDocsQuery(), dedupField="dedupField", clusterFields=[("termvector1", 1)], start=0, stop=10))
        self.assertEquals(4, len(result.hits))
        duplicates = [sorted([t['id'] for t in h.duplicates['topDocs']]) for h in result.hits]
        self.assertTrue('id:100' in [d for d in duplicates if 'id:0' in d][0])

        result = retval(self.lucene.executeQuery(MatchAllDocsQuery(), dedupField="dedupField", clusterFields=[("termvector1", 1), ("termvector2", 2)], start=0, stop=5))
        self.assertEquals(5, len(result.hits))
        duplicates = [sorted([t['id'] for t in h.duplicates['topDocs']]) for h in result.hits]
        self.assertFalse('id:100' in [d for d in duplicates if 'id:0' in d][0])

    def testCollectUntilStopWithForGrouping(self):
        for i in range(20):
            doc = Document()
            doc.add(NumericDocValuesField("__key__", long(42)))
            doc.add(StringField("sort", "%03d" % i, Field.Store.NO))
            consume(self.lucene.addDocument(identifier="id:%s" % i, document=doc))
        doc = Document()
        doc.add(StringField("sort", "100", Field.Store.NO))
        consume(self.lucene.addDocument(identifier="id:100", document=doc))

        result = retval(self.lucene.executeQuery(MatchAllDocsQuery(), groupingField="__key__", start=0, stop=2, sortKeys=[{'sortBy': 'sort', 'sortDescending': False}]))
        self.assertEqual(21, result.total)
        self.assertEqual(2, len(result.hits))

        result = retval(self.lucene.executeQuery(MatchAllDocsQuery(), groupingField="__key__", start=0, stop=5, sortKeys=[{'sortBy': 'sort', 'sortDescending': False}]))
        self.assertEqual(21, result.total)
        self.assertEqual(2, len(result.hits))

    def testReturnNoMoreThanStopForGrouping(self):
        for i in range(50):
            doc = Document()
            consume(self.lucene.addDocument(identifier="id:%s" % i, document=doc))
        doc = Document()
        consume(self.lucene.addDocument(identifier="id:100", document=doc))

        result = retval(self.lucene.executeQuery(MatchAllDocsQuery(), groupingField="__key__", start=5, stop=7, sortKeys=[{'sortBy': 'sort', 'sortDescending': False}]))
        self.assertEqual(51, result.total)
        self.assertEqual(2, len(result.hits))

    def testReturnNoMoreThanStopForClustering(self):
        for i in range(50):
            doc = Document()
            consume(self.lucene.addDocument(identifier="id:%s" % i, document=doc))
        doc = Document()
        consume(self.lucene.addDocument(identifier="id:100", document=doc))

        result = retval(self.lucene.executeQuery(MatchAllDocsQuery(), clusterFields=[("__key__", 1.0)], start=5, stop=7, sortKeys=[{'sortBy': 'sort', 'sortDescending': False}]))
        self.assertEqual(51, result.total)
        self.assertEqual(2, len(result.hits))

def facets(**fields):
    return [dict(fieldname=name, maxTerms=max_) for name, max_ in fields.items()]

def document(**fields):
    return createDocument(fields.items())

FIELD_REGISTRY = FieldRegistry(
    drilldownFields=[
            DrilldownField(name='field1'),
            DrilldownField(name='field2'),
            DrilldownField('field3'),
            DrilldownField('field_other', indexFieldName='other'),
            DrilldownField('fieldHier', hierarchical=True),
            DrilldownField('cat'),
        ]
    )
FIELD_REGISTRY.register(fieldname='intField', fieldDefinition=INTFIELD)

def createDocument(fields, facets=None):
    document = Document()
    for name, value in fields:
        document.add(FIELD_REGISTRY.createField(name, value))
    for facet, value in facets or []:
        if hasattr(value, 'extend'):
            path = [str(category) for category in value]
        else:
            path = [str(value)]
        document.add(FacetField(facet, path))
    return document

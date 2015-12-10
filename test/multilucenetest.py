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

from cqlparser import cqlToExpression
from meresco.components.json import JsonDict
from meresco.lucene import LuceneSettings, Lucene
from meresco.lucene.composedquery import ComposedQuery
from meresco.lucene.fieldregistry import FieldRegistry
from meresco.lucene.multilucene import MultiLucene
from meresco.lucene.queryexpressiontolucenequerydict import QueryExpressionToLuceneQueryDict
from seecr.test import SeecrTestCase
from weightless.core import consume, asList
from simplejson import loads


class MultiLuceneTest(SeecrTestCase):

    def setUp(self):
        SeecrTestCase.setUp(self)
        self.registry = FieldRegistry()

        self._multiLucene = MultiLucene(defaultCore='coreA', host="localhost", port=12345)
        self._lucene = Lucene(host="localhost", port=12345, settings=LuceneSettings(), name='coreA')
        self._multiLucene.addObserver(self._lucene)
        self.post = []
        self.response = ""
        def mockPost(data, path, **kwargs):
            self.post.append(dict(data=data, path=path))
            raise StopIteration(self.response)
            yield
        self._multiLucene._post = mockPost

    def testComposedQuery(self):
        self.response = JsonDict({
                "total": 887,
                "queryTime": 6,
                "hits": [{"id": "record:1", "score": 0.1234}]
            }).dumps()

        cq = ComposedQuery('coreA')
        q = QueryExpressionToLuceneQueryDict([], LuceneSettings()).convert(cqlToExpression("field=value"))
        cq.setCoreQuery("coreA", q)

        consume(self._multiLucene.executeComposedQuery(cq))
        self.assertEqual(1, len(self.post))
        self.assertEqual("/query/", self.post[0]['path'])
        self.assertEqual({
                "sortKeys": [],
                "resultsFrom": "coreA",
                "matches": {},
                "facets": {},
                "otherCoreFacetFilters": {},
                "rankQueries": {},
                "drilldownQueries": {},
                "unites": [],
                "queries": {"coreA": {"term": {"field": "field", "value": "value"}, "type": "TermQuery"}},
                "cores": ["coreA"],
                "filterQueries": {}
            }, loads(self.post[0]['data']))

    def testAddTypeAndMissingValueToSortField(self):
        self.response = JsonDict({
                "total": 887,
                "queryTime": 6,
                "hits": [{"id": "record:1", "score": 0.1234}]
            }).dumps()

        cq = ComposedQuery('coreA')
        q = QueryExpressionToLuceneQueryDict([], LuceneSettings()).convert(cqlToExpression("field=value"))
        cq.setCoreQuery('coreB', q)
        cq.sortKeys = [dict(sortBy='sortField', core='coreA', sortDescending=True)]
        cq.addMatch(dict(core='coreA', uniqueKey='A'), dict(core='coreB', key='B'))
        consume(self._multiLucene.executeComposedQuery(cq))
        self.assertEqual({
                "sortKeys": [{'core': 'coreA', 'sortBy': 'sortField', 'sortDescending': True, 'type': 'String', 'missingValue': 'STRING_FIRST'}],
                "resultsFrom": "coreA",
                'matches': {'coreA->coreB': [{'core': 'coreA', 'uniqueKey': 'A'}, {'core': 'coreB', 'key': 'B'}]},
                "facets": {},
                "otherCoreFacetFilters": {},
                "rankQueries": {},
                "drilldownQueries": {},
                "unites": [],
                'queries': {'coreB': {'term': {'field': 'field', 'value': 'value'}, 'type': 'TermQuery'}},
                "cores": ["coreB", "coreA"],
                "filterQueries": {}
            }, loads(self.post[0]['data']))

    def testCoreInfo(self):
        infos = asList(self._multiLucene.coreInfo())
        self.assertEquals(1, len(infos))
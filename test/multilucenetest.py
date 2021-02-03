## begin license ##
#
# "Meresco Lucene" is a set of components and tools to integrate Lucene into Meresco
#
# Copyright (C) 2013-2016, 2018, 2020-2021 Seecr (Seek You Too B.V.) https://seecr.nl
# Copyright (C) 2013-2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
# Copyright (C) 2015-2016 Koninklijke Bibliotheek (KB) http://www.kb.nl
# Copyright (C) 2016, 2020-2021 Stichting Kennisnet https://www.kennisnet.nl
# Copyright (C) 2021 Data Archiving and Network Services https://dans.knaw.nl
# Copyright (C) 2021 SURF https://www.surf.nl
# Copyright (C) 2021 The Netherlands Institute for Sound and Vision https://beeldengeluid.nl
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

from simplejson import loads

from cqlparser import cqlToExpression

from weightless.core import consume, asList, retval
from meresco.components.http.utils import parseResponse
from meresco.components.json import JsonDict

from meresco.lucene import LuceneSettings, Lucene
from meresco.lucene.composedquery import ComposedQuery
from meresco.lucene.fieldregistry import FieldRegistry
from meresco.lucene.multilucene import MultiLucene
from meresco.lucene.queryexpressiontolucenequerydict import QueryExpressionToLuceneQueryDict

from lucenetest import HTTP_RESPONSE


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
        connect = self._multiLucene._connect()
        connect._post = mockPost
        self._multiLucene._connect = lambda: connect

    def testInfoOnQuery(self):
        self.response = JsonDict({
                "total": 887,
                "queryTime": 6,
                "hits": [{"id": "record:1", "score": 0.1234}]
            }).dumps()

        q = ComposedQuery('coreA')
        q.addFilterQuery('coreB', query='N=true')
        q.addMatch(dict(core='coreA', uniqueKey='A'), dict(core='coreB', key='B'))
        result = retval(self._multiLucene.executeComposedQuery(q))
        self.assertEqual({
            'query': {
                'cores': ['coreB', 'coreA'],
                'drilldownQueries': {},
                'facets': {},
                'filterQueries': {'coreB': ['N=true']},
                'excludeFilterQueries': {},
                'matches': {'coreA->coreB': [{'core': 'coreA', 'uniqueKey': 'A'}, {'core': 'coreB', 'key': 'B'}]},
                'otherCoreFacetFilters': {},
                'queries': {},
                'rankQueries': {},
                'resultsFrom': 'coreA',
                'sortKeys': [],
                'unites': []
            },
            'type': 'ComposedQuery'
        }, result.info)

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
                "_sortKeys": [],
                "resultsFrom": "coreA",
                "_matches": {},
                "_facets": {},
                "_otherCoreFacetFilters": {},
                "_rankQueries": {},
                "_drilldownQueries": {},
                "_unites": [],
                "_queries": {"coreA": {"term": {"field": "field", "value": "value"}, "type": "TermQuery"}},
                "cores": ["coreA"],
                "_filterQueries": {},
                "_excludeFilterQueries": {},
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
                "_sortKeys": [{'core': 'coreA', 'sortBy': 'sortField', 'sortDescending': True, 'type': 'String', 'missingValue': 'STRING_FIRST'}],
                "resultsFrom": "coreA",
                '_matches': {'coreA->coreB': [{'core': 'coreA', 'uniqueKey': 'A'}, {'core': 'coreB', 'key': 'B'}]},
                "_facets": {},
                "_otherCoreFacetFilters": {},
                "_rankQueries": {},
                "_drilldownQueries": {},
                "_unites": [],
                '_queries': {'coreB': {'term': {'field': 'field', 'value': 'value'}, 'type': 'TermQuery'}},
                "cores": ["coreB", "coreA"],
                "_filterQueries": {},
                "_excludeFilterQueries": {},
            }, loads(self.post[0]['data']))

    def testCoreInfo(self):
        infos = asList(self._multiLucene.coreInfo())
        self.assertEqual(1, len(infos))

    def testLuceneServerHostPortDynamic(self):
        multiLucene = MultiLucene(defaultCore='core1')
        def httprequest1_1Mock(**kwargs):
            raise StopIteration(parseResponse(HTTP_RESPONSE))
            yield
        observer = CallTrace(
            'observer',
            returnValues=dict(luceneServer=('example.org', 1234)),
            methods=dict(httprequest1_1=httprequest1_1Mock))
        multiLucene.addObserver(observer)
        query = QueryExpressionToLuceneQueryDict([], LuceneSettings()).convert(cqlToExpression("field=value"))
        response = retval(multiLucene.executeComposedQuery(ComposedQuery('core1', query)))
        self.assertEqual(887, response.total)
        self.assertEqual(['luceneServer', 'httprequest1_1'], observer.calledMethodNames())

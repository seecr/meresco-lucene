## begin license ##
#
# "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
#
# Copyright (C) 2015 Koninklijke Bibliotheek (KB) http://www.kb.nl
# Copyright (C) 2015 Seecr (Seek You Too B.V.) http://seecr.nl
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

from seecr.test import SeecrTestCase
from seecr.utils.generatorutils import returnValueFromGenerator
from meresco.components.json import JsonDict, JsonList
from meresco.lucene import Lucene, LuceneSettings
from meresco.lucene.fieldregistry import FieldRegistry
from meresco.lucene.queryexpressiontolucenequerystring import QueryExpressionToLuceneQueryString
from weightless.core import consume
from cqlparser import cqlToExpression
from simplejson import loads

class LuceneTest(SeecrTestCase):

    def setUp(self):
        SeecrTestCase.setUp(self)
        self._lucene = Lucene(host="localhost", port=1234, name='lucene', settings=LuceneSettings())
        self.post = []
        self.response = ""
        def mockPost(data, path, **kwargs):
            self.post.append(dict(data=data, path=path))
            raise StopIteration(self.response)
            yield
        self._lucene._post = mockPost

    def testAdd(self):
        registry = FieldRegistry()
        fields = [registry.createField("id", "id1")]
        consume(self._lucene.addDocument(identifier='id1', fields=fields))
        self.assertEqual(1, len(self.post))
        self.assertEqual('/lucene/update/?identifier=id1', self.post[0]['path'])
        self.assertEqual('[{"type": "TextField", "name": "id", "value": "id1"}]', self.post[0]['data'])

    def testDelete(self):
        consume(self._lucene.delete(identifier='id1'))
        self.assertEqual(1, len(self.post))
        self.assertEqual('/lucene/delete/?identifier=id1', self.post[0]['path'])
        self.assertEqual(None, self.post[0]['data'])

    def testExecuteQuery(self):
        self.response = JsonDict({
                "total": 887,
                "queryTime": 6,
                "hits": [{"id": "record:1", "score": 0.1234}],
                "drilldownData": [
                    {"fieldname": "facet", "path": [], "terms": [{"term": "term", "count": 1}]}
                ]
            }).dumps()
        query = QueryExpressionToLuceneQueryString([], LuceneSettings()).convert(cqlToExpression("field=value"))
        response = returnValueFromGenerator(self._lucene.executeQuery(luceneQuery=query, start=1, stop=5, facets=[dict(maxTerms=10, fieldname='facet')], sortKeys=[dict(sortBy='field', sortDescending=False)]))
        self.assertEqual(1, len(self.post))
        self.assertEqual('/lucene/query/', self.post[0]['path'])
        self.assertEqual({
                    "start": 1, "stop": 5,
                    "query": {"term": {"field": "field", "value": "value"}, "type": "TermQuery"},
                    "facets": [{"fieldname": "facet", "maxTerms": 10}],
                    "sortKeys": [{"sortBy": "field", "sortDescending": False, "type": "String"}]
                }, loads(self.post[0]['data']))
        self.assertEqual(887, response.total)
        self.assertEqual(6, response.queryTime)
        self.assertEqual(1, len(response.hits))
        self.assertEqual("record:1", response.hits[0].id)
        self.assertEqual(0.1234, response.hits[0].score)
        self.assertEqual([
                {"fieldname": "facet", "path": [], "terms": [{"term": "term", "count": 1}]}
            ], response.drilldownData)

    def testPrefixSearch(self):
        self.response = JsonList([["value0", 1], ["value1", 2]]).dumps()
        response = returnValueFromGenerator(self._lucene.prefixSearch(fieldname='field1', prefix='valu'))
        self.assertEquals(['value1', 'value0'], response.hits)

        response = returnValueFromGenerator(self._lucene.prefixSearch(fieldname='field1', prefix='valu', showCount=True))
        self.assertEquals([('value1', 2), ('value0', 1)], response.hits)

    def testNumDocs(self):
        self.response = "150"
        result = returnValueFromGenerator(self._lucene.numDocs())
        self.assertEqual(150, result)
        self.assertEqual([{'data': None, 'path': '/lucene/numDocs/'}], self.post)
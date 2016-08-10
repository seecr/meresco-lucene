## begin license ##
#
# "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
#
# Copyright (C) 2015-2016 Koninklijke Bibliotheek (KB) http://www.kb.nl
# Copyright (C) 2015-2016 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2016 Stichting Kennisnet http://www.kennisnet.nl
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
from meresco.components.json import JsonDict, JsonList
from meresco.lucene import Lucene, LuceneSettings
from meresco.lucene.fieldregistry import FieldRegistry
from meresco.lucene.queryexpressiontolucenequerydict import QueryExpressionToLuceneQueryDict
from weightless.core import consume, retval
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
        self._lucene._connect._post = mockPost

        self.read = []
        self.response = ""
        def mockRead(path, **kwargs):
            self.read.append(path)
            raise StopIteration(self.response)
            yield
        self._lucene._connect.read = mockRead

    def testPostSettingsAddObserverInit(self):
        self.assertEqual([], self.post)
        self._lucene.observer_init()
        self.assertEqual(1, len(self.post))
        self.assertEqual('/lucene/settings/', self.post[0]['path'])
        self.assertEqual('{"lruTaxonomyWriterCacheSize": 4000, "maxMergeAtOnce": 2, "similarity": {"type": "BM25Similarity"}, "numberOfConcurrentTasks": 6, "segmentsPerTier": 8.0, "analyzer": {"type": "MerescoStandardAnalyzer"}, "drilldownFields": [], "commitCount": 100000, "commitTimeout": 10}', self.post[0]['data'])

    def testInitialize(self):
        self.assertEqual([], self.post)
        consume(self._lucene.initialize())
        self.assertEqual(1, len(self.post))
        self.assertEqual('/lucene/settings/', self.post[0]['path'])
        self.assertEqual('{"lruTaxonomyWriterCacheSize": 4000, "maxMergeAtOnce": 2, "similarity": {"type": "BM25Similarity"}, "numberOfConcurrentTasks": 6, "segmentsPerTier": 8.0, "analyzer": {"type": "MerescoStandardAnalyzer"}, "drilldownFields": [], "commitCount": 100000, "commitTimeout": 10}', self.post[0]['data'])

    def testAdd(self):
        registry = FieldRegistry()
        fields = [registry.createField("id", "id1")]
        consume(self._lucene.addDocument(identifier='id1', fields=fields))
        self.assertEqual(1, len(self.post))
        self.assertEqual('/lucene/update/?identifier=id1', self.post[0]['path'])
        self.assertEqual('[{"type": "TextField", "name": "id", "value": "id1"}]', self.post[0]['data'])

    def testAddWithoutIdentifier(self):
        registry = FieldRegistry()
        fields = [registry.createField("id", "id1")]
        consume(self._lucene.addDocument(fields=fields))
        self.assertEqual(1, len(self.post))
        self.assertEqual('/lucene/update/?', self.post[0]['path'])
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
                "times": {"searchTime": 3},
                "hits": [{
                        "id": "record:1", "score": 0.1234,
                        "duplicateCount": {"__key__": 2},
                        "duplicates": {"__grouping_key__": [{"id": 'record:1'}, {"id": 'record:2'}]}
                    }],
                "drilldownData": [
                    {"fieldname": "facet", "path": [], "terms": [{"term": "term", "count": 1}]}
                ],
                "suggestions": {
                    "valeu": ["value"]
                }
            }).dumps()
        query = QueryExpressionToLuceneQueryDict([], LuceneSettings()).convert(cqlToExpression("field=value"))
        response = retval(self._lucene.executeQuery(
                    luceneQuery=query, start=1, stop=5,
                    facets=[dict(maxTerms=10, fieldname='facet')],
                    sortKeys=[dict(sortBy='field', sortDescending=False)],
                    suggestionRequest=dict(suggests=['valeu'], count=2, field='field1'),
                    dedupField="__key__",
                    groupingField="__grouping_key__",
                    clustering=True,
                    storedFields=["field"]
                ))
        self.assertEqual(1, len(self.post))
        self.assertEqual('/lucene/query/', self.post[0]['path'])
        self.assertEqual({
                    "start": 1, "stop": 5,
                    "storedFields": ["field"],
                    "query": {"term": {"field": "field", "value": "value"}, "type": "TermQuery"},
                    "facets": [{"fieldname": "facet", "maxTerms": 10}],
                    "sortKeys": [{"sortBy": "field", "sortDescending": False, "type": "String", 'missingValue': 'STRING_LAST'}],
                    "suggestionRequest": dict(suggests=['valeu'], count=2, field='field1'),
                    "dedupField": "__key__",
                    "dedupSortField": None,
                    "groupingField": "__grouping_key__",
                    "clustering": True,
                }, loads(self.post[0]['data']))
        self.assertEqual(887, response.total)
        self.assertEqual(6, response.queryTime)
        self.assertEqual({'searchTime': 3}, response.times)
        self.assertEqual(1, len(response.hits))
        self.assertEqual("record:1", response.hits[0].id)
        self.assertEqual(0.1234, response.hits[0].score)
        self.assertEqual(dict(__key__=2), response.hits[0].duplicateCount)
        self.assertEqual([
                {"fieldname": "facet", "path": [], "terms": [{"term": "term", "count": 1}]}
            ], response.drilldownData)
        self.assertEqual({'valeu': ['value']}, response.suggestions)

    def testPrefixSearch(self):
        self.response = JsonList([["value0", 1], ["value1", 2]]).dumps()
        response = retval(self._lucene.prefixSearch(fieldname='field1', prefix='valu'))
        self.assertEquals(['value1', 'value0'], response.hits)

        response = retval(self._lucene.prefixSearch(fieldname='field1', prefix='valu', showCount=True))
        self.assertEquals([('value1', 2), ('value0', 1)], response.hits)

    def testNumDocs(self):
        self.response = "150"
        result = retval(self._lucene.numDocs())
        self.assertEqual(150, result)
        self.assertEqual([{'data': None, 'path': '/lucene/numDocs/'}], self.post)

    def testFieldnames(self):
        self.response = '["field1", "field2"]'
        result = retval(self._lucene.fieldnames())
        self.assertEqual(["field1", "field2"], result.hits)
        self.assertEqual([{"data": None, "path": "/lucene/fieldnames/"}], self.post)

    def testDrilldownFieldnames(self):
        self.response = '["field1", "field2"]'
        result = retval(self._lucene.drilldownFieldnames())
        self.assertEqual(["field1", "field2"], result.hits)
        self.assertEqual([{"data": None, "path": "/lucene/drilldownFieldnames/?limit=50"}], self.post)

        result = retval(self._lucene.drilldownFieldnames(limit=1, path=['field']))
        self.assertEqual(["field1", "field2"], result.hits)
        self.assertEqual({"data": None, "path": "/lucene/drilldownFieldnames/?dim=field&limit=1"}, self.post[-1])

        result = retval(self._lucene.drilldownFieldnames(limit=1, path=['xyz', 'abc', 'field']))
        self.assertEqual(["field1", "field2"], result.hits)
        self.assertEqual({"data": None, "path": "/lucene/drilldownFieldnames/?dim=xyz&limit=1&path=abc&path=field"}, self.post[-1])

    def testUpdateSettings(self):
        self.response = JsonDict(numberOfConcurrentTasks=6, similarity="BM25(k1=1.2,b=0.75)", clustering=JsonDict(clusterMoreRecords=100, clusteringEps=0.4, clusteringMinPoints=1))
        settings = retval(self._lucene.getSettings())
        self.assertEqual(['/settings/'], self.read)
        self.assertEquals({'numberOfConcurrentTasks': 6, 'similarity': u'BM25(k1=1.2,b=0.75)', 'clustering': {'clusterMoreRecords': 100, 'clusteringEps': 0.4, 'clusteringMinPoints': 1}}, settings)

        clusterFields = [
            {"filterValue": None, "fieldname": "untokenized.dcterms:isFormatOf.uri", "weight": 0}
        ]
        self.response = ""
        consume(self._lucene.setSettings(similarity=dict(name="bm25", k1=1.0, b=2.0), numberOfConcurrentTasks=10, clustering=dict(clusterMoreRecords=200, clusteringEps=1.0, clusteringMinPoints=2, fields=clusterFields)))
        self.assertEqual(1, len(self.post))
        self.assertEqual('/lucene/settings/', self.post[0]['path'])
        self.assertEqual({
                "numberOfConcurrentTasks": 10,
                "similarity": dict(type="BM25Similarity", k1=1.0, b=2.0),
                "clustering": {
                    "clusterMoreRecords": 200,
                    "clusteringEps": 1.0,
                    "clusteringMinPoints": 2,
                    "fields": [
                        {"filterValue": None, "fieldname": "untokenized.dcterms:isFormatOf.uri", "weight": 0}
                    ]
                }
            }, loads(self.post[0]['data']))

        consume(self._lucene.setSettings(numberOfConcurrentTasks=5, similarity=None, clustering=None))
        self.assertEqual(2, len(self.post))
        self.assertEqual('/lucene/settings/', self.post[1]['path'])
        self.assertEqual({
                "numberOfConcurrentTasks": 5,
            }, loads(self.post[1]['data']))

    def testSimilarDocs(self):
        self.response = JsonDict({
                "total": 887,
                "queryTime": 6,
                "times": {"searchTime": 3},
                "hits": [
                        {"id": "record:1", "score": 0.1234},
                        {"id": "record:2", "score": 0.1234},
                    ],
            }).dumps()
        response = retval(self._lucene.similarDocuments(identifier='record:3'))
        self.assertEqual(887, response.total)
        self.assertEqual(2, len(response.hits))

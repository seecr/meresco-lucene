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

from meresco.lucene.remote import LuceneRemote, LuceneRemoteService
from meresco.lucene import LuceneResponse
from meresco.core import Observable
from seecr.test import SeecrTestCase, CallTrace
from cqlparser import parseString
from weightless.core import compose
from seecr.utils.generatorutils import returnValueFromGenerator
from simplejson import loads, dumps

class LuceneRemoteTest(SeecrTestCase):
    def testRemoteExecuteQuery(self):
        http = CallTrace('http')
        def httppost(*args, **kwargs):
            raise StopIteration('HTTP/1.0 200 Ok\r\n\r\n%s' % LuceneResponse(total=5, hits=["1", "2", "3", "4", "5"]).asJson())
            yield
        http.methods['httppost'] = httppost
        remote = LuceneRemote(host='host', port=1234, path='/path')
        observable = Observable()
        observable.addObserver(remote)
        remote._httppost = http.httppost

        result = returnValueFromGenerator(observable.any.executeQuery(
                cqlAbstractSyntaxTree=parseString('query AND  field=value'),
                start=0,
                stop=10,
                facets=[{'fieldname': 'field', 'maxTerms':5}],
                filterQueries=[parseString('query=fiets')],
                joinQueries=[
                    {
                        "core": 'core1',
                        "fromField": 'A.joinfield',
                        "toField": 'B.joinfield',
                        "query": parseString('query=test')
                    }
                ]
            )
        )
        self.assertEquals(5, result.total)
        self.assertEquals(["1", "2", "3", "4", "5"], result.hits)

        self.assertEquals(['httppost'], http.calledMethodNames())
        m = http.calledMethods[0]
        self.assertEquals('host', m.kwargs['host'])
        self.assertEquals(1234, m.kwargs['port'])
        self.assertEquals('/path/__lucene_remote__', m.kwargs['request'])
        self.assertEquals('application/json', m.kwargs['headers']['Content-Type'])
        self.assertDictEquals({
                'message': 'executeQuery',
                'kwargs':{
                    'cqlQuery': 'query AND field=value',
                    'start':0,
                    'stop': 10,
                    'facets': [{'fieldname': 'field', 'maxTerms':5}],
                    'filterQueries': ['query=fiets'],
                    'joinQueries': [
                        {
                            "core": 'core1',
                            "fromField": 'A.joinfield',
                            "toField": 'B.joinfield',
                            "query": 'query=test'
                        }
                    ]
                }
            }, loads(m.kwargs['body']))

    def testRemotePrefixSearch(self):
        http = CallTrace('http')
        def httppost(*args, **kwargs):
            raise StopIteration('HTTP/1.0 200 Ok\r\n\r\n%s' % LuceneResponse(total=5, hits=["1", "2", "3", "4", "5"]).asJson())
            yield
        http.methods['httppost'] = httppost
        remote = LuceneRemote(host='host', port=1234, path='/path')
        observable = Observable()
        observable.addObserver(remote)
        remote._httppost = http.httppost

        result = returnValueFromGenerator(observable.any.prefixSearch(prefix='aap', fieldname='field', limit=10))
        self.assertEquals(5, result.total)
        self.assertEquals(['httppost'], http.calledMethodNames())
        m = http.calledMethods[0]
        self.assertEquals('host', m.kwargs['host'])
        self.assertEquals({
                'message': 'prefixSearch',
                'kwargs':{
                    'prefix':'aap',
                    'fieldname': 'field',
                    'limit': 10,
                }
            }, loads(m.kwargs['body']))

    def testRemoteFieldnames(self):
        http = CallTrace('http')
        def httppost(*args, **kwargs):
            raise StopIteration('HTTP/1.0 200 Ok\r\n\r\n%s' % LuceneResponse(total=2, hits=["field0", "field1"]).asJson())
            yield
        http.methods['httppost'] = httppost
        remote = LuceneRemote(host='host', port=1234, path='/path')
        observable = Observable()
        observable.addObserver(remote)
        remote._httppost = http.httppost

        result = returnValueFromGenerator(observable.any.fieldnames())
        self.assertEquals(2, result.total)
        self.assertEquals(['httppost'], http.calledMethodNames())
        m = http.calledMethods[0]
        self.assertEquals('host', m.kwargs['host'])
        self.assertEquals({
                'message': 'fieldnames',
                'kwargs':{
                }
            }, loads(m.kwargs['body']))


    def testServiceExecuteQuery(self):
        observer = CallTrace('lucene')
        def executeQuery(**kwargs):
            raise StopIteration(LuceneResponse(total=2, hits=['aap','noot']))
            yield
        observer.methods['executeQuery'] = executeQuery
        service = LuceneRemoteService(CallTrace('reactor'))
        service.addObserver(observer)
        body = dumps({
                'message': 'executeQuery',
                'kwargs':{
                    'cqlQuery': 'query AND field=value',
                    'start':0,
                    'stop': 10,
                    'facets': [{'fieldname': 'field', 'maxTerms':5}],
                    'filterQueries': ['query=fiets'],
                    'joinQueries': [
                        {
                            "core": 'core1',
                            "fromField": 'A.joinfield',
                            "toField": 'B.joinfield',
                            "query": 'query=test'
                        }
                    ]
                }
            })
        result = ''.join(compose(service.handleRequest(path='/__lucene_remote__', Method="POST", Body=body)))
        header, body = result.split('\r\n'*2)
        self.assertTrue('Content-Type: application/json' in header, header)
        response = LuceneResponse.fromJson(body)
        self.assertEquals(2, response.total)
        self.assertEquals(['aap', 'noot'], response.hits)
        self.assertEquals(['executeQuery'], observer.calledMethodNames())
        m = observer.calledMethods[0]
        self.assertEquals(parseString('query AND field=value'), m.kwargs['cqlAbstractSyntaxTree'])
        self.assertEquals(0, m.kwargs['start'])
        self.assertEquals(10, m.kwargs['stop'])
        self.assertEquals([{'fieldname': 'field', 'maxTerms':5}], m.kwargs['facets'])
        self.assertEquals([parseString('query=fiets')], m.kwargs['filterQueries'])
        self.assertEquals([
                {
                    "core": 'core1',
                    "fromField": 'A.joinfield',
                    "toField": 'B.joinfield',
                    "query": parseString('query=test')
                }
            ], m.kwargs['joinQueries'])

    def testServicePrefixSearch(self):
        observer = CallTrace('lucene')
        def prefixSearch(**kwargs):
            raise StopIteration(LuceneResponse(total=2, hits=['aap','noot']))
            yield
        observer.methods['prefixSearch'] = prefixSearch
        service = LuceneRemoteService(CallTrace('reactor'))
        service.addObserver(observer)
        body = dumps({
                'message': 'prefixSearch',
                'kwargs':{
                    'prefix':'aap',
                    'fieldname': 'field',
                    'limit': 10,
                }
            })
        result = ''.join(compose(service.handleRequest(path='/__lucene_remote__', Method="POST", Body=body)))
        header, body = result.split('\r\n'*2)
        self.assertTrue('Content-Type: application/json' in header, header)
        response = LuceneResponse.fromJson(body)
        self.assertEquals(2, response.total)
        self.assertEquals(['aap', 'noot'], response.hits)
        self.assertEquals(['prefixSearch'], observer.calledMethodNames())
        m = observer.calledMethods[0]
        self.assertEquals('aap', m.kwargs['prefix'])
        self.assertEquals(10, m.kwargs['limit'])
        self.assertEquals('field', m.kwargs['fieldname'])

    def testServiceFieldnames(self):
        observer = CallTrace('lucene')
        def fieldnames(**kwargs):
            raise StopIteration(LuceneResponse(total=2, hits=['aap','noot']))
            yield
        observer.methods['fieldnames'] = fieldnames
        service = LuceneRemoteService(CallTrace('reactor'))
        service.addObserver(observer)
        body = dumps({
                'message': 'fieldnames',
                'kwargs':{
                }
            })
        result = ''.join(compose(service.handleRequest(path='/__lucene_remote__', Method="POST", Body=body)))
        header, body = result.split('\r\n'*2)
        self.assertTrue('Content-Type: application/json' in header, header)
        response = LuceneResponse.fromJson(body)
        self.assertEquals(2, response.total)
        self.assertEquals(['aap', 'noot'], response.hits)
        self.assertEquals(['fieldnames'], observer.calledMethodNames())
        m = observer.calledMethods[0]



## begin license ##
#
# "Meresco Lucene" is a set of components and tools to integrate Lucene into Meresco
#
# Copyright (C) 2013-2015, 2021 Seecr (Seek You Too B.V.) https://seecr.nl
# Copyright (C) 2013-2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
# Copyright (C) 2015 Koninklijke Bibliotheek (KB) http://www.kb.nl
# Copyright (C) 2021 Data Archiving and Network Services https://dans.knaw.nl
# Copyright (C) 2021 SURF https://www.surf.nl
# Copyright (C) 2021 Stichting Kennisnet https://www.kennisnet.nl
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

from meresco.lucene.remote import LuceneRemote, LuceneRemoteService
from meresco.lucene import LuceneResponse
from meresco.lucene.composedquery import ComposedQuery
from meresco.lucene.hit import Hit
from meresco.core import Observable
from seecr.test import SeecrTestCase, CallTrace
from cqlparser import parseString
from weightless.core import compose, retval
from simplejson import loads, dumps
from meresco.lucene.remote._conversion import Conversion


class LuceneRemoteTest(SeecrTestCase):
    def testRemoteExecuteQuery(self):
        http = CallTrace('http')
        def httppost(*args, **kwargs):
            raise StopIteration('HTTP/1.0 200 Ok\r\n\r\n%s' % LuceneResponse(total=5, hits=[Hit("1"), Hit("2"), Hit("3", duplicateCount=2), Hit("4"), Hit("5")]).asJson())
            yield
        http.methods['httppost'] = httppost
        remote = LuceneRemote(host='host', port=1234, path='/path')
        observable = Observable()
        observable.addObserver(remote)
        remote._httppost = http.httppost

        cq = ComposedQuery('coreA')
        cq.setCoreQuery(
                core='coreA',
                query=parseString('query AND  field=value'),
                filterQueries=[parseString('query=fiets')],
                facets=[{'fieldname': 'field', 'maxTerms':5}],
            )
        cq.setCoreQuery(core='coreB', query=parseString('query=test'))
        cq.addMatch(dict(core='coreA', uniqueKey='keyA'), dict(core='coreB', key='keyB'))
        result = retval(observable.any.executeComposedQuery(query=cq))
        self.assertEqual(5, result.total)
        self.assertEqual([Hit("1"), Hit("2"), Hit("3", duplicateCount=2), Hit("4"), Hit("5")], result.hits)

        self.assertEqual(['httppost'], http.calledMethodNames())
        m = http.calledMethods[0]
        self.assertEqual('host', m.kwargs['host'])
        self.assertEqual(1234, m.kwargs['port'])
        self.assertEqual('/path/__lucene_remote__', m.kwargs['request'])
        self.assertEqual('application/json', m.kwargs['headers']['Content-Type'])
        message, kwargs = Conversion().jsonLoadMessage(m.kwargs['body'])
        query = kwargs['query']
        self.assertEqual('executeComposedQuery', message)
        self.assertEqual('coreA', query.resultsFrom)
        self.assertEqual([{'fieldname': 'field', 'maxTerms':5}], query.facetsFor('coreA'))

    def testRemoteExecuteQueryWithNoneValues(self):
        http = CallTrace('http')
        def httppost(*args, **kwargs):
            raise StopIteration('HTTP/1.0 200 Ok\r\n\r\n%s' % LuceneResponse(total=5, hits=[Hit("1"), Hit("2"), Hit("3"), Hit("4"), Hit("5")]).asJson())
            yield
        http.methods['httppost'] = httppost
        remote = LuceneRemote(host='host', port=1234, path='/path')
        observable = Observable()
        observable.addObserver(remote)
        remote._httppost = http.httppost

        result = retval(observable.any.executeQuery(
                cqlAbstractSyntaxTree=parseString('query AND  field=value'),
                start=0,
                stop=10,
                facets=None,
                filterQueries=None,
                joinQueries=None,
            )
        )
        self.assertEqual(5, result.total)
        self.assertEqual([Hit("1"), Hit("2"), Hit("3"), Hit("4"), Hit("5")], result.hits)

        self.assertEqual(['httppost'], http.calledMethodNames())
        m = http.calledMethods[0]
        self.assertEqual('host', m.kwargs['host'])
        self.assertEqual(1234, m.kwargs['port'])
        self.assertEqual('/path/__lucene_remote__', m.kwargs['request'])
        self.assertEqual('application/json', m.kwargs['headers']['Content-Type'])
        self.assertDictEquals({
                'message': 'executeQuery',
                'kwargs':{
                    'cqlAbstractSyntaxTree': {'__CQL_QUERY__': 'query AND field=value'},
                    'start':0,
                    'stop': 10,
                    'facets': None,
                    'filterQueries': None,
                    'joinQueries': None,
                }
            }, loads(m.kwargs['body']))

    def testDeclineOtherMessages(self):
        class Other(object):
            def aMessage(self):
                raise StopIteration('Thanks')
                yield
        remote = LuceneRemote(host='host', port=1234, path='/path')
        observable = Observable()
        observable.addObserver(remote)
        observable.addObserver(Other())
        result = retval(observable.any.aMessage())
        self.assertEqual('Thanks', result)

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

        result = retval(observable.any.prefixSearch(prefix='aap', fieldname='field', limit=10))
        self.assertEqual(5, result.total)
        self.assertEqual(['httppost'], http.calledMethodNames())
        m = http.calledMethods[0]
        self.assertEqual('host', m.kwargs['host'])
        self.assertEqual({
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

        result = retval(observable.any.fieldnames())
        self.assertEqual(2, result.total)
        self.assertEqual(['httppost'], http.calledMethodNames())
        m = http.calledMethods[0]
        self.assertEqual('host', m.kwargs['host'])
        self.assertEqual({
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
                    'cqlAbstractSyntaxTree': {'__CQL_QUERY__': 'query AND field=value'},
                    'start':0,
                    'stop': 10,
                    'facets': [{'fieldname': 'field', 'maxTerms':5}],
                    'filterQueries': [{'__CQL_QUERY__': 'query=fiets'}],
                    'joinQueries': {'core1': {'__CQL_QUERY__': 'query=test'}}
                }
            })
        result = ''.join(compose(service.handleRequest(path='/__lucene_remote__', Method="POST", Body=body)))
        header, body = result.split('\r\n'*2)
        self.assertTrue('Content-Type: application/json' in header, header+body)
        response = LuceneResponse.fromJson(body)
        self.assertEqual(2, response.total)
        self.assertEqual(['aap', 'noot'], response.hits)
        self.assertEqual(['executeQuery'], observer.calledMethodNames())
        m = observer.calledMethods[0]
        self.assertEqual(parseString('query AND field=value'), m.kwargs['cqlAbstractSyntaxTree'])
        self.assertEqual(0, m.kwargs['start'])
        self.assertEqual(10, m.kwargs['stop'])
        self.assertEqual([{'fieldname': 'field', 'maxTerms':5}], m.kwargs['facets'])
        self.assertEqual([parseString('query=fiets')], m.kwargs['filterQueries'])
        self.assertEqual({'core1': parseString('query=test')}, m.kwargs['joinQueries'])

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
        self.assertEqual(2, response.total)
        self.assertEqual(['aap', 'noot'], response.hits)
        self.assertEqual(['prefixSearch'], observer.calledMethodNames())
        m = observer.calledMethods[0]
        self.assertEqual('aap', m.kwargs['prefix'])
        self.assertEqual(10, m.kwargs['limit'])
        self.assertEqual('field', m.kwargs['fieldname'])

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
        self.assertEqual(2, response.total)
        self.assertEqual(['aap', 'noot'], response.hits)
        self.assertEqual(['fieldnames'], observer.calledMethodNames())

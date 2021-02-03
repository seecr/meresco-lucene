## begin license ##
#
# "Meresco Lucene" is a set of components and tools to integrate Lucene into Meresco
#
# Copyright (C) 2015-2016 Koninklijke Bibliotheek (KB) http://www.kb.nl
# Copyright (C) 2015-2016, 2021 Seecr (Seek You Too B.V.) https://seecr.nl
# Copyright (C) 2016, 2021 Stichting Kennisnet https://www.kennisnet.nl
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

from simplejson import loads

from cqlparser import parseString
from cqlparser.cqltoexpression import QueryExpression

from seecr.test import SeecrTestCase

from meresco.lucene import ComposedQuery
from meresco.lucene.remote import Conversion


class ConversionTest(SeecrTestCase):
    def testConversion(self):
        kwargs = {'q': parseString('CQL'), 'attr': {'qs':[parseString('qs')]}}
        dump = Conversion().jsonDumpMessage(message='aMessage', **kwargs)
        self.assertEqual(str, type(dump))
        message, kwargs = Conversion().jsonLoadMessage(dump)
        self.assertEqual('aMessage', message)
        self.assertEqual(parseString('CQL'), kwargs['q'])
        self.assertEqual([parseString('qs')], kwargs['attr']['qs'])

    def testConversionOfComposedQuery(self):
        conversion = Conversion()
        cq = ComposedQuery('coreA')
        cq.setCoreQuery(core='coreA', query=parseString('Q0'), filterQueries=['Q1', 'Q2'], facets=['F0', 'F1'])
        cq.setCoreQuery(core='coreB', query=QueryExpression.searchterm(term='Q3'), filterQueries=['Q4'])
        cq.addMatch(dict(core='coreA', uniqueKey='keyA'), dict(core='coreB', key='keyB'))
        cq.addUnite(dict(core='coreA', query='AQuery'), dict(core='coreB', query='anotherQuery'))
        cq.start = 0
        cq.sortKeys = [dict(sortBy='field', sortDescending=True)]

        kwargs = {'q': cq}
        dump = conversion.jsonDumpMessage(message='aMessage', **kwargs)
        self.assertEqual(str, type(dump))
        message, kwargs = conversion.jsonLoadMessage(dump)
        self.assertEqual('aMessage', message)
        cq2 = kwargs['q']
        self.assertEqual(parseString('Q0'), cq2.queryFor('coreA'))

    def testQueryExpression(self):
        conversion = Conversion()
        kwargs = {'q': QueryExpression.searchterm(term='term')}
        dump = conversion.jsonDumpMessage(message='aMessage', **kwargs)
        loadedMessage, loadedKwargs = conversion.jsonLoadMessage(dump)
        self.assertEqual('aMessage', loadedMessage)
        self.assertEqual({'q': QueryExpression.searchterm(term='term')}, loadedKwargs)

    def testQueryExpressionWithOperands(self):
        conversion = Conversion()
        qe = QueryExpression.nested(operator='AND')
        qe.operands = [QueryExpression.searchterm(term='term'), QueryExpression.searchterm(term='term1')]
        kwargs = {'q': qe}
        dump = conversion.jsonDumpMessage(message='aMessage', **kwargs)
        loadedMessage, loadedKwargs = conversion.jsonLoadMessage(dump)
        self.assertEqual('aMessage', loadedMessage)
        self.assertEqual({'q': qe}, loadedKwargs)

    def testSpecialObject(self):
        class MyObject():
            def asDict(self):
                return {"this":"dict"}
            @classmethod
            def fromDict(cls, aDict):
                self.assertEqual({"this": "dict"}, aDict)
                return cls()

        conversion = Conversion()
        conversion._addObject('__MyObject__', MyObject, MyObject.asDict, MyObject.fromDict)
        kwargs = {'q': "query", 'object': MyObject()}
        dump = conversion.jsonDumpMessage(message='aMessage', **kwargs)
        self.assertEqual({
                'kwargs': {
                    'object': {
                        '__MyObject__': '{"this": "dict"}'
                    },
                    'q': 'query'
                },
                'message':'aMessage'
            }, loads(dump))
        loadedMessage, loadedKwargs = conversion.jsonLoadMessage(dump)
        self.assertEqual('aMessage', loadedMessage)
        self.assertEqual('query', loadedKwargs['q'])
        self.assertTrue(isinstance(loadedKwargs['object'], MyObject))

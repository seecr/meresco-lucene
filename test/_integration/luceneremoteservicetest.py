## begin license ##
#
# "Meresco Lucene" is a set of components and tools to integrate Lucene into Meresco
#
# Copyright (C) 2013-2016, 2018-2019, 2021 Seecr (Seek You Too B.V.) https://seecr.nl
# Copyright (C) 2013-2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
# Copyright (C) 2015-2016 Koninklijke Bibliotheek (KB) http://www.kb.nl
# Copyright (C) 2016, 2018, 2021 Stichting Kennisnet https://www.kennisnet.nl
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

from lxml.etree import HTML, tostring

from seecr.test import IntegrationTestCase
from seecr.test.utils import getRequest

from meresco.lucene.synchronousremote import SynchronousRemote
from cqlparser.cqltoexpression import cqlToExpression


class LuceneRemoteServiceTest(IntegrationTestCase):
    def testRemoteService(self):
        remote = SynchronousRemote(host='localhost', port=self.httpPort, path='/remote')
        response = remote.executeQuery(cqlToExpression('*'))
        self.assertEqual(100, response.total)

    def testRemoteServiceOnBadPath(self):
        remote = SynchronousRemote(host='localhost', port=self.httpPort, path='/does/not/exist')
        self.assertRaises(IOError, lambda: remote.executeQuery(cqlToExpression('*')))

    def testRemoteServiceWithBadCore(self):
        remote = SynchronousRemote(host='localhost', port=self.httpPort, path='/remote')
        self.assertRaises(IOError, lambda: remote.executeQuery(cqlToExpression('*'), core='doesnotexist'))

    def testRemoteInfo(self):
        statusAndHeaders, body = getRequest(port=self.httpPort, path='/remote/info/index')
        body = tostring(body, encoding=str)
        self.assertTrue('main' in body, body)
        self.assertTrue('empty-core' in body, body)

    def testRemoteInfoStatic(self):
        statusAndHeaders, body = getRequest(port=self.httpPort, path='/remote/info/static/lucene-info.css')
        self.assertEqual('200', statusAndHeaders['StatusCode'])

    def testRemoteInfoRedirect(self):
        statusAndHeaders, body = getRequest(port=self.httpPort, path='/remote/info')
        self.assertEqual('/remote/info/index', statusAndHeaders['Headers']['Location'])

    def testRemoteInfoCore(self):
        statusAndHeaders, body = getRequest(port=self.httpPort, path='/remote/info/core', arguments=dict(name='main'))
        text = tostring(body, encoding=str)
        self.assertFalse('Traceback' in text, text)
        lists = body.xpath('//ul')
        fieldList = lists[0]
        fields = fieldList.xpath('li/a/text()')
        self.assertEqual(19, len(fields))
        self.assertEqual([
                '$facets',
                '__id__',
                '__key__.field',
                'copy',
                'field1',
                'field2',
                'field3',
                'field4',
                'field5',
                'field_missing',
                'intfield1',
                'intfield2',
                'intfield3',
                'intfield_missing',
                'sorted.field2',
                'sorted.field4',
                'sorted.intfield1',
                'sorted.intfield_missing',
                'untokenized.field3',
            ], fields)

        drilldownFieldList = lists[1]
        drilldownFields = drilldownFieldList.xpath('li/a/text()')
        self.assertEqual(set(['untokenized.field2', 'untokenized.fieldHier', 'untokenized.field2.copy']), set(drilldownFields))

        # TODO: Show sorted fields

    def testRemoteInfoField(self):
        statusAndHeaders, body = getRequest(port=self.httpPort, path='/remote/info/field', arguments=dict(fieldname='__id__', name='main'), parse=False)
        body = body.decode()
        self.assertFalse('Traceback' in body, body)
        self.assertEqual(50, body.count(': 1'), body)

    def testRemoteInfoFieldWithPrefix(self):
        statusAndHeaders, body = getRequest(port=self.httpPort, path='/remote/info/field', arguments=dict(fieldname='field2', name='main', prefix='value8'), parse=False)
        body = body.decode()
        self.assertTrue("<pre>0 value8: 10\n</pre>" in body, body)

    def testRemoteInfoDrilldownValues(self):
        statusAndHeaders, body = getRequest(port=self.httpPort, path='/remote/info/drilldownvalues', arguments=dict(path='untokenized.field2', name='main'))
        self.assertFalse('Traceback' in tostring(body, encoding=str), tostring(body, encoding=str))
        self.assertEqual(set(['value1', 'value0', 'value9', 'value8', 'value7', 'value6', 'value5', 'value4', 'value3', 'othervalue2', 'value2']), set(body.xpath('//ul/li/a/text()')))


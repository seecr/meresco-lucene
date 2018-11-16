## begin license ##
#
# "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
#
# Copyright (C) 2013-2016, 2018 Seecr (Seek You Too B.V.) https://seecr.nl
# Copyright (C) 2013-2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
# Copyright (C) 2015-2016 Koninklijke Bibliotheek (KB) http://www.kb.nl
# Copyright (C) 2016, 2018 Stichting Kennisnet https://www.kennisnet.nl
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

from lxml.etree import HTML

from seecr.test import IntegrationTestCase
from seecr.test.utils import getRequest

from meresco.lucene.synchronousremote import SynchronousRemote
from cqlparser.cqltoexpression import cqlToExpression


class LuceneRemoteServiceTest(IntegrationTestCase):
    def testRemoteService(self):
        remote = SynchronousRemote(host='localhost', port=self.httpPort, path='/remote')
        response = remote.executeQuery(cqlToExpression('*'))
        self.assertEquals(100, response.total)

    def testRemoteServiceOnBadPath(self):
        remote = SynchronousRemote(host='localhost', port=self.httpPort, path='/does/not/exist')
        self.assertRaises(IOError, lambda: remote.executeQuery(cqlToExpression('*')))

    def testRemoteServiceWithBadCore(self):
        remote = SynchronousRemote(host='localhost', port=self.httpPort, path='/remote')
        self.assertRaises(IOError, lambda: remote.executeQuery(cqlToExpression('*'), core='doesnotexist'))

    def testRemoteInfo(self):
        header, body = getRequest(port=self.httpPort, path='/remote/info/index', parse=False)
        self.assertTrue('main' in body, body)
        self.assertTrue('empty-core' in body, body)

    def testRemoteInfoStatic(self):
        header, body = getRequest(port=self.httpPort, path='/remote/info/static/lucene-info.css', parse=False)
        self.assertTrue('200' in header, header)

    def testRemoteInfoRedirect(self):
        header, body = getRequest(port=self.httpPort, path='/remote/info', parse=False)
        headerLines = header.split('\r\n')
        self.assertTrue('Location: /remote/info/index' in headerLines, header)

    def testRemoteInfoCore(self):
        header, body = getRequest(port=self.httpPort, path='/remote/info/core', arguments=dict(name='main'), parse=False)
        self.assertFalse('Traceback' in body, body)  # only tested for MultiLucene situation for now!
        bodyLxml = HTML(body)
        lists = bodyLxml.xpath('//ul')
        fieldList = lists[0]
        fields = fieldList.xpath('li/a/text()')
        self.assertEquals(19, len(fields))
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
        self.assertEquals(set(['untokenized.field2', 'untokenized.fieldHier', 'untokenized.field2.copy']), set(drilldownFields))

        # TODO: Show sorted fields

    def testRemoteInfoField(self):
        header, body = getRequest(port=self.httpPort, path='/remote/info/field', arguments=dict(fieldname='__id__', name='main'), parse=False)
        self.assertFalse('Traceback' in body, body)
        self.assertEquals(50, body.count(': 1'), body)

    def testRemoteInfoFieldWithPrefix(self):
        header, body = getRequest(port=self.httpPort, path='/remote/info/field', arguments=dict(fieldname='field2', name='main', prefix='value8'), parse=False)
        self.assertTrue("<pre>value8: 10</pre>" in body, body)

    def testRemoteInfoDrilldownValues(self):
        header, body = getRequest(port=self.httpPort, path='/remote/info/drilldownvalues', arguments=dict(path='untokenized.field2', name='main'), parse=False)
        self.assertFalse('Traceback' in body, body)
        bodyLxml = HTML(body)
        self.assertEquals(set(['value1', 'value0', 'value9', 'value8', 'value7', 'value6', 'value5', 'value4', 'value3', 'othervalue2', 'value2']), set(bodyLxml.xpath('//ul/li/a/text()')))

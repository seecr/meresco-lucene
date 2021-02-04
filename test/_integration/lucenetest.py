## begin license ##
#
# "Meresco Lucene" is a set of components and tools to integrate Lucene into Meresco
#
# Copyright (C) 2013-2016, 2018-2019, 2021 Seecr (Seek You Too B.V.) https://seecr.nl
# Copyright (C) 2013-2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
# Copyright (C) 2015-2016, 2019 Koninklijke Bibliotheek (KB) http://www.kb.nl
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

from seecr.test import IntegrationTestCase
from seecr.test.utils import getRequest, postRequest
from meresco.xml.namespaces import namespaces as _namespaces
from simplejson import loads
from time import sleep
from meresco.lucene.fieldregistry import KEY_PREFIX
from meresco.lucene import ComposedQuery
from meresco.lucene.synchronousremote import SynchronousRemote
from cqlparser import parseString
from cqlparser.cqltoexpression import cqlToExpression

namespaces = _namespaces.copyUpdate(dict(example='http://meresco.org/namespace/example'))
xpath = namespaces.xpath
xpathFirst = namespaces.xpathFirst


class LuceneTest(IntegrationTestCase):
    def testAddDelete(self):
        postRequest(self.httpPort, '/update_main', ADD_RECORD, parse=False)
        sleep(1.1)
        try:
            self.assertEqual(1, self.numberOfRecords(query='__id__ exact "testrecord:1"'))
        finally:
            postRequest(self.httpPort, '/update_main', DELETE_RECORD, parse=False)
        sleep(1.1)
        self.assertEqual(0, self.numberOfRecords(query='__id__ exact "testrecord:1"'))

    def testQuery(self):
        self.assertEqual(10, self.numberOfRecords(query='field2=value2'))
        self.assertEqual(2, self.numberOfRecords(query='field1=value1'))
        self.assertEqual(100, self.numberOfRecords(query='*'))

    def testQueryViaRemote(self):
        self.assertEqual(10, self.numberOfRecords(query='field2=value2', path='/via-remote-sru'))
        self.assertEqual(2, self.numberOfRecords(query='field1=value1', path='/via-remote-sru'))
        self.assertEqual(100, self.numberOfRecords(query='*', path='/via-remote-sru'))

    def testRecordIds(self):
        body = self.doSruQuery('*', maximumRecords=100)
        records = xpath(body, '//srw:recordIdentifier/text()')
        self.assertEqual(100, len(records))
        self.assertEqual(set(['record:%s' % i for i in range(1,101)]), set(records))

    def testMaximumRecords(self):
        body = self.doSruQuery('*', maximumRecords=20)
        records = xpath(body, '//srw:record')
        self.assertEqual(20, len(records))

        body = self.doSruQuery('*', maximumRecords=0, path='/via-remote-sru')
        records = xpath(body, '//srw:record')
        self.assertEqual(0, len(records))
        diag = xpathFirst(body, '//diag:diagnostic/diag:details/text()')
        self.assertEqual(None, diag)

    def testStartRecord(self):
        body = self.doSruQuery('*', maximumRecords=100)
        records = xpath(body, '//srw:recordIdentifier/text()')
        body = self.doSruQuery('*', maximumRecords=10, startRecord=51)
        self.assertEqual(records[50:60], xpath(body, '//srw:recordIdentifier/text()'))

    def testSortKeys(self):
        body = self.doSruQuery('*', sortKeys='sorted.intfield1,,1')
        records = xpath(body, '//srw:recordIdentifier/text()')
        self.assertEqual(['record:%s' % i for i in range(1,11)], records)

        body = self.doSruQuery('*', sortKeys='sorted.intfield1,,0')
        records = xpath(body, '//srw:recordIdentifier/text()')
        self.assertEqual(['record:%s' % i for i in range(100,90, -1)], records)

    def testSortKeysWithMissingValues(self):
        body = self.doSruQuery('*', sortKeys='sorted.field4,,1')
        records = xpath(body, '//srw:recordIdentifier/text()')
        self.assertEqual('record:1', records[0])

        body = self.doSruQuery('*', sortKeys='sorted.field4,,0')
        records = xpath(body, '//srw:recordIdentifier/text()')
        self.assertEqual('record:1', records[0])

        body = self.doSruQuery('field_missing=test', sortKeys='sorted.intfield_missing,,0')
        self.assertEqual('10', xpathFirst(body, '/srw:searchRetrieveResponse/srw:numberOfRecords/text()'))
        def missing_intfields(body):
            values = []
            for rec in xpath(body, '//srw:record/srw:recordData'):
                values.append(xpathFirst(rec, 'example:document/example:intfield_missing/text()'))
            return values
        self.assertEqual(['66775', '187', '64', '42', '17', '-5', '-308', None, None, None], missing_intfields(body))
        body = self.doSruQuery('field_missing=test', sortKeys='sorted.intfield_missing,,1')
        self.assertEqual(['-308', '-5', '17', '42', '64', '187', '66775', None, None, None], missing_intfields(body))

    def testFacet(self):
        body = self.doSruQuery('*', facet='untokenized.field2')
        ddItems = xpath(body, "//drilldown:term-drilldown/drilldown:navigator[@name='untokenized.field2']/drilldown:item")
        self.assertEqual(
            set([('value0', '10'), ('value9', '10'), ('value8', '10'), ('value7', '10'), ('value6', '10'), ('value5', '10'), ('value4', '10'), ('value3', '10'), ('value2', '10'), ('value1', '9')]),
            set([(i.attrib['value'], i.attrib['count']) for i in ddItems]))

    def testFacet2Copy(self):
        body = self.doSruQuery('*', facet='untokenized.field2.copy')
        ddItems = xpath(body, "//drilldown:term-drilldown/drilldown:navigator[@name='untokenized.field2.copy']/drilldown:item")
        self.assertEqual(
            set([('value0', '10'), ('value9', '10'), ('value8', '10'), ('value7', '10'), ('value6', '10'), ('value5', '10'), ('value4', '10'), ('value3', '10'), ('value2', '10'), ('value1', '9')]),
            set([(i.attrib['value'], i.attrib['count']) for i in ddItems]))

    def testAutocomplete(self):
        status, header, body = getRequest(port=self.httpPort, path='/autocomplete', arguments={'field': 'field2', 'prefix': 'va'})
        prefix, completions = body
        self.assertEqual("va", prefix)

        self.assertEqual(set(["value0", "value2", "value3", "value4", "value1"]), set(completions))
        self.assertEqual('value1', completions[-1])

    def testJoin(self):
        remote = SynchronousRemote(host='localhost', port=self.httpPort, path='/remote')
        q = ComposedQuery('main', query=cqlToExpression('*'))
        q.addMatch(dict(core='main', uniqueKey=KEY_PREFIX+'field'), dict(core='main2', key=KEY_PREFIX+'field'))
        q.start=0
        q.stop=100
        q.addFilterQuery(core='main', query=cqlToExpression('field2=value0 OR field2=value1'))
        q.addFacet(core='main2', facet=dict(fieldname='untokenized.field2', maxTerms=5))
        response = remote.executeComposedQuery(query=q)
        self.assertEqual(19, response.total)
        self.assertEqual(set([
                'record:10', 'record:11', 'record:20', 'record:21', 'record:30',
                'record:31', 'record:40', 'record:41', 'record:50', 'record:51',
                'record:60', 'record:61', 'record:70', 'record:71', 'record:80',
                'record:81', 'record:90', 'record:91', 'record:100'
            ]), set([hit.id for hit in response.hits]))
        self.assertEqual([{
                'fieldname': 'untokenized.field2',
                'path': [],
                'terms': [
                    {'count': 27, 'term': 'value3'},
                    {'count': 22, 'term': 'value0'},
                    {'count': 19, 'term': 'value5'},
                    {'count': 19, 'term': 'value7'},
                    {'count': 19, 'term': 'value9'},
                ]
            }], response.drilldownData)

    def testJoinWithSortAndMissingValue(self):
        remote = SynchronousRemote(host='localhost', port=self.httpPort, path='/remote')
        q = ComposedQuery('main', query=cqlToExpression('*'))
        q.addMatch(dict(core='main', uniqueKey=KEY_PREFIX+'field'), dict(core='main2', key=KEY_PREFIX+'field'))
        q.addFacet(core='main2', facet=dict(fieldname='untokenized.field2', maxTerms=5))
        q.addSortKey(dict(core="main", sortBy="sorted.field4", sortDescending=True))
        response = remote.executeComposedQuery(query=q)
        self.assertEqual("record:1", response.hits[0].id)
        del q._sortKeys[:]
        q.addSortKey(dict(core="main", sortBy="sorted.field4", sortDescending=False))
        response = remote.executeComposedQuery(query=q)
        self.assertEqual("record:1", response.hits[0].id)


    def testDedup(self):
        remote = SynchronousRemote(host='localhost', port=self.httpPort, path='/remote')
        response = remote.executeQuery(cqlAbstractSyntaxTree=parseString('*'), dedupField="__key__.field", core="main", stop=3)
        self.assertEqual(100, response.total)
        self.assertEqual(100, response.totalWithDuplicates)
        self.assertEqual(
            [1, 1, 1],
            [hit.duplicateCount['__key__.field'] for hit in response.hits]
        )

        response = remote.executeQuery(cqlAbstractSyntaxTree=parseString('*'), dedupField="__key__.groupfield", dedupSortField="__id__", core="main2", stop=3)
        self.assertEqual(3, len(response.hits))
        self.assertEqual(10, response.total)
        self.assertEqual(1000, response.totalWithDuplicates)
        self.assertEqual(
            [100] * 3,
            [hit.duplicateCount['__key__.groupfield'] for hit in response.hits]
        )

        response = remote.executeQuery(cqlAbstractSyntaxTree=parseString('*'), dedupField="__key__.groupfield", dedupSortField="__numeric__.sort1", core="main2", stop=100000)
        self.assertEqual(10, len(response.hits))
        self.assertEqual(10, response.total)
        self.assertEqual(1000, response.totalWithDuplicates)
        self.assertEqual(
            [100] * 10,
            [hit.duplicateCount['__key__.groupfield'] for hit in response.hits]
        )

        response = remote.executeQuery(cqlAbstractSyntaxTree=parseString('groupfield=1'), dedupField="__key__.groupfield", dedupSortField=["__numeric__.sort1","__numeric__.sort2"], core="main2", stop=10000)
        self.assertEqual(1, len(response.hits))
        self.assertEqual(1, response.total)
        self.assertEqual(100, response.totalWithDuplicates)
        self.assertEqual(['main2:record:199'], [hit.id for hit in response.hits]
        )

        response = remote.executeQuery(cqlAbstractSyntaxTree=parseString('groupfield=1'), dedupField="__key__.groupfield", dedupSortField=["__numeric__.sort2","__numeric__.sort1"], core="main2", stop=10000)
        self.assertEqual(1, len(response.hits))
        self.assertEqual(1, response.total)
        self.assertEqual(100, response.totalWithDuplicates)
        self.assertEqual(['main2:record:199'], [hit.id for hit in response.hits]
        )

    def testDutchStemming(self):
        self.assertEqual(1, self.numberOfRecords("field5=katten"))
        self.assertEqual(1, self.numberOfRecords("field4=kat"))

    def testFieldHierarchicalDrilldown(self):
        response = self.doSruQuery('*', facet='untokenized.fieldHier', drilldownFormat='json')
        json = xpathFirst(response, '/srw:searchRetrieveResponse/srw:extraResponseData/drilldown:drilldown/drilldown:term-drilldown/drilldown:json/text()')
        result = loads(json)
        self.assertEqual(1, len(result))
        drilldown = result[0]
        self.assertEqual('untokenized.fieldHier', drilldown['fieldname'])
        self.assertEqual(set([('parent0', 50), ('parent1', 50)]), set([(t['term'], t['count']) for t in drilldown['terms']]))
        self.assertEqual(set([('child0', 17), ('child1', 17), ('child2', 16)]), set([(t['term'], t['count']) for t in drilldown['terms'][0]['subterms']]))

    def testFieldHierarchicalSearch(self):
        response = self.doSruQuery('untokenized.fieldHier exact "parent0>child1>grandchild2"', facet='untokenized.fieldHier', drilldownFormat='json')
        self.assertEqual('3', xpathFirst(response, '/srw:searchRetrieveResponse/srw:numberOfRecords/text()'))

    def xtestQueryAfterRestartDoesReInitSettings(self):
        self.assertEqual(10, self.numberOfRecords(query='field2=value2'))
        self.stopServer("lucene-server")
        self.startLuceneServer()
        self.assertEqual(10, self.numberOfRecords(query='field2=value2'))

    def doSruQuery(self, query, maximumRecords=None, startRecord=None, sortKeys=None, facet=None, path='/sru', drilldownFormat='xml'):
        arguments={'version': '1.2',
            'operation': 'searchRetrieve',
            'query': query,
        }
        if maximumRecords is not None:
            arguments['maximumRecords'] = maximumRecords
        if startRecord is not None:
            arguments['startRecord'] = startRecord
        if sortKeys is not None:
            arguments["sortKeys"] =sortKeys
        if facet is not None:
            arguments["x-term-drilldown"] = facet
        arguments['x-drilldown-format'] = drilldownFormat
        status, header, body = getRequest(port=self.httpPort, path=path, arguments=arguments )
        return body

    def numberOfRecords(self, query, path='/sru'):
        body = self.doSruQuery(query, path=path)
        result = xpathFirst(body, '/srw:searchRetrieveResponse/srw:numberOfRecords/text()')
        return None if result is None else int(result)

ADD_RECORD = """
<ucp:updateRequest xmlns:ucp="info:lc/xmlns/update-v1">
    <srw:version xmlns:srw="http://www.loc.gov/zing/srw/">1.0</srw:version>
    <ucp:action>info:srw/action/1/replace</ucp:action>
    <ucp:recordIdentifier>testrecord:1</ucp:recordIdentifier>
    <srw:record xmlns:srw="http://www.loc.gov/zing/srw/">
        <srw:recordPacking>xml</srw:recordPacking>
        <srw:recordSchema>data</srw:recordSchema>
        <srw:recordData><document xmlns='http://meresco.org/namespace/example'>
    <field1>value1</field1>
</document></srw:recordData>
    </srw:record>
</ucp:updateRequest>
"""

DELETE_RECORD = """
<ucp:updateRequest xmlns:ucp="info:lc/xmlns/update-v1">
    <srw:version xmlns:srw="http://www.loc.gov/zing/srw/">1.0</srw:version>
    <ucp:action>info:srw/action/1/delete</ucp:action>
    <ucp:recordIdentifier>testrecord:1</ucp:recordIdentifier>
    <srw:record xmlns:srw="http://www.loc.gov/zing/srw/">
        <srw:recordPacking>xml</srw:recordPacking>
        <srw:recordSchema>data</srw:recordSchema>
        <srw:recordData/>
    </srw:record>
</ucp:updateRequest>
"""

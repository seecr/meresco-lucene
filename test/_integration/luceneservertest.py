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

from seecr.test import IntegrationTestCase
from seecr.test.utils import postRequest
from meresco.components.json import JsonList, JsonDict
from simplejson import loads

class LuceneServerTest(IntegrationTestCase):

    def testAddAndQueryDocument(self):
        data = JsonList([
                {"type": "TextField", "name": "fieldname", "value": "value"}
            ]).dumps()
        header, body = postRequest(self.serverPort, '/update?identifier=id1', data=data)
        self.assertTrue("200 OK" in header.upper(), header)

        header, body = postRequest(self.serverPort, '/query', data=JsonDict(query=dict(type="MatchAllDocsQuery")).dumps(), parse=False)
        self.assertTrue("200 OK" in header.upper(), header)
        response = loads(body)
        self.assertEqual(1, response['total'])
        self.assertEqual([{'id': 'id1'}], response['hits'])

        header, body = postRequest(self.serverPort, '/query', data=JsonDict(query=dict(type="TermQuery", term=dict(field="fieldname", value="value"))).dumps(), parse=False)
        self.assertTrue("200 OK" in header.upper(), header)
        response = loads(body)
        self.assertEqual(1, response['total'])
        self.assertEqual([{'id': 'id1'}], response['hits'])

    def testFacets(self):
        data = JsonList([
                {"type": "TextField", "name": "fieldname", "value": "value"},
                {"type": "FacetField", "name": "fieldname", "path": ["value"]}
            ]).dumps()
        header, body = postRequest(self.serverPort, '/update?identifier=id1', data=data)
        self.assertTrue("200 OK" in header.upper(), header)

        header, body = postRequest(self.serverPort, '/query', data=JsonDict(query=dict(type="MatchAllDocsQuery"), facets=[{"fieldname": "fieldname", "maxTerms": 10}]).dumps(), parse=False)
        self.assertTrue("200 OK" in header.upper(), header)
        response = loads(body)
        self.assertEqual(1, response['total'])
        self.assertEqual([{'path': [], 'fieldname': 'fieldname', 'terms': [{'count': 1, 'term': 'value'}]}], response['drilldownData'])


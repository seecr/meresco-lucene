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


from meresco.core import Observable
from cqlparser import cql2string
from zp.utils import xpathFirst, xpath
from lxml.etree import XML
from urllib import urlencode
from weightless.http import httpget
from simplejson import loads

from meresco.lucene import LuceneResponse


class LuceneRemote(Observable):
    def __init__(self, host=None, port=None, path=None, name=None):
        Observable.__init__(self, name=name)
        self._host = host
        self._port = port
        self._path = '' if path is None else path

    def executeQuery(self, cqlAbstractSyntaxTree, start=0, stop=10, sortKeys=None, suggestionsCount=0, suggestionsQuery=None, filterQueries=None, joinQueries=None, facets=None, joinFacets=None, **kwargs):

        arguments = dict(operation='searchRetrieve', version='1.2', recordSchema='irrelevant', query=cql2string(cqlAbstractSyntaxTree))
        arguments['startRecord'] = start + 1
        arguments['maximumRecords'] = stop - start

        if sortKeys:
            arguments["sortKeys"] = ["%s,,%s" % (sortKey['sortBy'], '0' if sortKey['sortDescending'] else '1') for sortKey in sortKeys]
        if facets:
            arguments["x-drilldown-format"] = "json"
            arguments["x-term-drilldown"] = ['%(fieldname)s:%(maxTerms)s' % facet for facet in facets]

        remoteResponse = yield self._read(self._path, arguments)
        sruResponse = XML(remoteResponse)
        response = LuceneResponse()
        response.total = int(xpathFirst(sruResponse, '/srw:searchRetrieveResponse/srw:numberOfRecords/text()'))
        response.hits = xpath(sruResponse, '/srw:searchRetrieveResponse/srw:records/srw:record/srw:recordIdentifier/text()')
        json = xpathFirst(sruResponse, '/srw:searchRetrieveResponse/srw:extraResponseData/drilldown:drilldown/drilldown:term-drilldown/drilldown:json/text()')
        if json:
            response.drilldownData = loads(json)
        raise StopIteration(response)


    def _luceneRemoteServer(self):
        return (self._host, self._port) if self._host else self.call.luceneRemoteServer()

    def _read(self, path, arguments=None):
        host, port = self._luceneRemoteServer()
        if arguments:
            path += '?' + urlencode(arguments, doseq=True)
        response = yield self._httpget(host, port, path)
        header, body = response.split('\r\n\r\n', 1)
        self._verify200(header, response)
        raise StopIteration(body)

    def _httpget(self, *args):
        return httpget(*args)

    def _verify200(self, header, response):
        if not header.startswith('HTTP/1.0 200'):
            raise IOError("Expected status '200' from LuceneRemoteService, but got: " + response)

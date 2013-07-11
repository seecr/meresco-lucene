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
from cqlparser import cql2string, parseString
from meresco.xml import xpathFirst, xpath
from lxml.etree import XML
from urllib import urlencode
from weightless.http import httppost
from simplejson import loads, dumps

from meresco.lucene import LuceneResponse
from meresco.components.http.utils import Ok, CRLF, ContentTypeHeader


class LuceneRemote(Observable):
    def __init__(self, host=None, port=None, path=None, name=None):
        Observable.__init__(self, name=name)
        self._host = host
        self._port = port
        self._path = '' if path is None else path

    # def executeQuery(self, cqlAbstractSyntaxTree, start=0, stop=10, sortKeys=None, suggestionsCount=0, suggestionsQuery=None, filterQueries=None, joinQueries=None, facets=None, joinFacets=None, **kwargs):
    def executeQuery(self, cqlAbstractSyntaxTree, **kwargs):
        kwargDict = kwargs.copy()
        kwargDict["cqlQuery"] = cql2string(cqlAbstractSyntaxTree)
        messageDict = dict(message="executeQuery", kwargs=kwargDict)
        remoteResponse = yield self._send(dumps(messageDict))
        raise StopIteration(LuceneResponse.fromJson(remoteResponse))

    def _luceneRemoteServer(self):
        return (self._host, self._port) if self._host else self.call.luceneRemoteServer()

    def _send(self, body, contentType="application/json"):
        headers = None
        if body:
             headers={'Content-Type': contentType, 'Content-Length': len(body)}
        host, port = self._luceneRemoteServer() # WARNING: can return a different server each time.
        response = yield self._httppost(host=host, port=port, request=self._path, body=body, headers=headers)
        header, body = response.split("\r\n\r\n", 1)
        self._verify200(header, response)
        raise StopIteration(body)

    def _httppost(self, **kwargs):
        return httppost(**kwargs)

    def _verify200(self, header, response):
        if not header.startswith('HTTP/1.0 200'):
            raise IOError("Expected status '200' from LuceneRemoteService, but got: " + response)

class LuceneRemoteService(Observable):
    def handleRequest(self, Body, **kwargs):
        messasgeDict = loads(Body)
        if messasgeDict['message'] != 'executeQuery':
            raise ValueError('Expected "executeQuery"')
        kwargs = messasgeDict['kwargs']
        kwargs['cqlAbstractSyntaxTree'] = parseString(kwargs.pop('cqlQuery'))
        response = yield self.any.executeQuery(**kwargs)
        yield Ok
        yield ContentTypeHeader + 'application/json' + CRLF
        yield CRLF
        yield response.asJson()
## begin license ##
#
# "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
#
# Copyright (C) 2013-2015 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2013-2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
# Copyright (C) 2015 Koninklijke Bibliotheek (KB) http://www.kb.nl
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

from weightless.core import DeclineMessage
from weightless.http import httppost
from meresco.components.http.utils import CRLF
from meresco.components.json import JsonDict
from meresco.core import Observable

from seecr.utils.generatorutils import generatorReturn

from simplejson import loads
from urllib2 import urlopen
from _lucene2 import luceneResponseFromDict


class MultiLucene(Observable):
    def __init__(self, host, port, defaultCore):
        Observable.__init__(self)
        self._host = host
        self._port = port
        self._defaultCore = defaultCore

    def executeQuery(self, core=None, **kwargs):
        coreName = self._defaultCore if core is None else core
        response = yield self.any[coreName].executeQuery(**kwargs)
        generatorReturn(response)

    def executeComposedQuery(self, query):
        jsonDict = JsonDict()
        for k, v in query.asDict().items():
            jsonDict[k.replace("_", "")] = v
        for sortKey in query.sortKeys:
            coreName = sortKey.get('core', query.resultsFrom)
            self.call[coreName].updateSortKey(sortKey)
        responseDict = (yield self._send(jsonDict=jsonDict, path='/query/'))
        raise StopIteration(luceneResponseFromDict(responseDict))
        yield

    def _send(self, path, jsonDict=None):
        response = yield self._post(path=path, data=jsonDict.dumps() if jsonDict else None)
        raise StopIteration(loads(response) if response else None)

    def _post(self, path, data):
        response = yield httppost(host=self._host, port=self._port, request=path, body=data)
        header, body = response.split(CRLF * 2, 1)
        self._verify20x(header, response)
        raise StopIteration(body)

    def _verify20x(self, header, response):
        if not header.startswith('HTTP/1.1 20'):
            raise IOError("Expected status 'HTTP/1.1 20x' from Lucene server, but got: " + response)

    def any_unknown(self, message, **kwargs):
        if message in ['prefixSearch', 'fieldnames', 'drilldownFieldnames']:
            core = kwargs.get('core')
            if core is None:
                core = self._defaultCore
            result = yield self.any[core].unknown(message=message, **kwargs)
            raise StopIteration(result)
        raise DeclineMessage()

    def coreInfo(self):
        yield self.all.coreInfo()


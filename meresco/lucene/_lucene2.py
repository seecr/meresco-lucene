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

from urllib import urlencode
from meresco.components.http.utils import CRLF
from meresco.components.json import JsonList, JsonDict
from meresco.core import Observable
from meresco.lucene import LuceneResponse
from meresco.lucene.hit import Hit
from weightless.core import consume
from weightless.http import httppost
from simplejson import loads
from urllib2 import urlopen

class Lucene(Observable):

    def __init__(self, host, port, settings, name, **kwargs):
        Observable.__init__(self, name=name, **kwargs)
        self._host = host
        self._port = port
        self._settings = settings
        self._name = name

    def observer_init(self):
        consume(self._send(jsonDict=JsonDict(commitCount=self._settings.commitCount, commitTimeout=self._settings.commitTimeout), path="/settings/", synchronous=True))

    def addDocument(self, identifier, fields):
        yield self._send(jsonDict=JsonList(fields), path='/update/?{}'.format(urlencode(dict(identifier=identifier))))

    def delete(self, identifier):
        yield self._send(path='/delete/?{}'.format(urlencode(dict(identifier=identifier))))

    def executeQuery(self, luceneQuery, start=None, stop=None, facets=None, **kwargs):
        jsonDict = JsonDict(
            query=loads(luceneQuery),
            start=start,
            stop=stop
        )
        responseDict = (yield self._send(jsonDict=jsonDict, path='/query/'))
        hits = [Hit(**hit) for hit in responseDict['hits']]
        response = LuceneResponse(total=responseDict["total"], queryTime=responseDict["queryTime"], hits=hits)
        raise StopIteration(response)

    def _send(self, path, jsonDict=None, synchronous=False):
        path = "/" + self._name + path
        response = yield self._post(path=path, data=jsonDict.dumps() if jsonDict else None, synchronous=synchronous)
        raise StopIteration(loads(response) if response else None)

    def _post(self, path, data, synchronous=False):
        if synchronous:
            body = urlopen("http://{}:{}{}".format(self._host, self._port, path), data=data).read()
        else:
            response = yield httppost(host=self._host, port=self._port, request=path, body=data)
            header, body = response.split(CRLF * 2, 1)
            self._verify20x(header, response)
        raise StopIteration(body)

    def _verify20x(self, header, response):
        if not header.startswith('HTTP/1.1 20'):
            raise IOError("Expected status 'HTTP/1.1 20x' from Lucene server, but got: " + response)

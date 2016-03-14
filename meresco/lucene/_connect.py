## begin license ##
#
# "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
#
# Copyright (C) 2016 Seecr (Seek You Too B.V.) http://seecr.nl
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

from urllib2 import urlopen

from simplejson import loads

from meresco.components.http.utils import CRLF


class _Connect(object):
    def __init__(self, host, port, observable, pathPrefix=None):
        self._host = host
        self._port = port
        self._pathPrefix = pathPrefix or ''
        self._observable = observable

    def send(self, path, jsonDict=None, synchronous=False):
        response = yield self._post(path=self._pathPrefix + path, data=jsonDict.dumps() if jsonDict else None, synchronous=synchronous)
        raise StopIteration(loads(response) if response else None)

    def read(self, path):
        response = yield self._get(path=self._pathPrefix + path)
        raise StopIteration(loads(response) if response else None)

    def _post(self, path, data, synchronous=False):
        if synchronous:
            body = urlopen("http://{}:{}{}".format(self._host, self._port, path), data=data).read()
        else:
            response = yield self._observable.any.httprequest(method='POST', host=self._host, port=self._port, request=path, body=data)
            header, body = response.split(CRLF * 2, 1)
            self._verify20x(header, response)
        raise StopIteration(body)

    def _get(self, path):
        response = yield self._observable.any.httprequest(method='GET', host=self._host, port=self._port, request=path)
        header, body = response.split(CRLF * 2, 1)
        self._verify20x(header, response)
        raise StopIteration(body)

    def _verify20x(self, header, response):
        if not header.startswith('HTTP/1.1 20'):
            raise IOError("Expected status 'HTTP/1.1 20x' from Lucene server, but got: " + response)

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


from simplejson import loads



class _Connect(object):
    def __init__(self, host, port, observable, uninitializedCallback=None, pathPrefix=None):
        self._host = host
        self._port = port
        self._pathPrefix = pathPrefix or ''
        self._observable = observable
        self._uninitializedCallback = uninitializedCallback or (lambda: None)

    def send(self, path, jsonDict=None):
        post = lambda: self._post(path=self._pathPrefix + path, data=jsonDict.dumps() if jsonDict else None)
        try:
            body = yield post()
        except UninitializedException:
            yield self._uninitializedCallback()
            body = yield post()
        raise StopIteration(loads(body) if body else None)

    def read(self, path):
        get = yield self._get(path=self._pathPrefix + path)
        try:
            body = yield get()
        except UninitializedException:
            yield self._uninitializedCallback()
            body = yield get()
        raise StopIteration(loads(body) if body else None)

    def _post(self, path, data):
        statusAndHeaders, body = yield self._observable.any.httprequest1_1(method='POST', host=self._host, port=self._port, request=path, body=data)
        self._verify20x(statusAndHeaders, body)
        raise StopIteration(body)

    def _get(self, path):
        statusAndHeaders, body = yield self._observable.any.httprequest1_1(method='GET', host=self._host, port=self._port, request=path)
        self._verify20x(statusAndHeaders, body)
        raise StopIteration(body)

    def _verify20x(self, statusAndHeaders, body):
        if statusAndHeaders['StatusCode'] == "409":
            raise UninitializedException()
        if not statusAndHeaders['StatusCode'].startswith('20'):
            raise IOError("Expected status '20x' from Lucene server, but got {} {}\n{}".format(statusAndHeaders['StatusCode'], statusAndHeaders['ReasonPhrase'], body))

class UninitializedException(Exception):
    pass
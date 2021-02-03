## begin license ##
#
# "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
#
# Copyright (C) 2016 Koninklijke Bibliotheek (KB) http://www.kb.nl
# Copyright (C) 2016, 2018 Seecr (Seek You Too B.V.) https://seecr.nl
# Copyright (C) 2016 Stichting Kennisnet http://www.kennisnet.nl
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
    def __init__(self, host, port, observable, pathPrefix=None):
        self._host = host
        self._port = port
        self._pathPrefix = pathPrefix or ''
        self._observable = observable

    def send(self, path, jsonDict=None, data=None, parse=True):
        post = lambda: self._post(path=self._pathPrefix + path, data=jsonDict.dumps() if jsonDict else data)
        try:
            body = yield post()
        except UninitializedException:
            yield self._observable.initialize()
            body = yield post()
        if body and parse:
            body = loads(body)
        return body if body is not None else None

    def read(self, path, parse=True):
        get = lambda: self._get(path=self._pathPrefix + path)
        try:
            body = yield get()
        except UninitializedException:
            yield self._observable.initialize()
            body = yield get()
        if body and parse:
            body = loads(body)
        return body if body is not None else None

    def _post(self, path, data):
        statusAndHeaders, body = yield self._observable.any.httprequest1_1(method='POST', host=self._host, port=self._port, request=path, body=data)
        self._verify20x(statusAndHeaders, body)
        return body

    def _get(self, path):
        statusAndHeaders, body = yield self._observable.any.httprequest1_1(method='GET', host=self._host, port=self._port, request=path)
        self._verify20x(statusAndHeaders, body)
        return body

    def _verify20x(self, statusAndHeaders, body):
        if statusAndHeaders['StatusCode'] == "409":
            raise UninitializedException()
        if not statusAndHeaders['StatusCode'].startswith('20'):
            raise IOError("Expected status '20x' from Lucene server, but got {} {}\n{}".format(statusAndHeaders['StatusCode'], statusAndHeaders['ReasonPhrase'], body))


class UninitializedException(Exception):
    pass

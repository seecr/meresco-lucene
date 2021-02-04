## begin license ##
#
# "Meresco Lucene" is a set of components and tools to integrate Lucene into Meresco
#
# Copyright (C) 2013, 2015-2016, 2021 Seecr (Seek You Too B.V.) https://seecr.nl
# Copyright (C) 2013 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
# Copyright (C) 2015-2016 Koninklijke Bibliotheek (KB) http://www.kb.nl
# Copyright (C) 2016, 2021 Stichting Kennisnet https://www.kennisnet.nl
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


from meresco.core import Observable
from weightless.http import httppost
from weightless.core import DeclineMessage

from meresco.lucene import LuceneResponse

from ._conversion import Conversion


class LuceneRemote(Observable):
    def __init__(self, host=None, port=None, path=None, name=None, conversion=None):
        Observable.__init__(self, name=name)
        self._host = host
        self._port = port
        self._path = '' if path is None else path
        self._path += '/__lucene_remote__'
        self._conversion = Conversion() if conversion is None else conversion

    def any_unknown(self, message, **kwargs):
        if message in self._ALLOWED_METHODS:
            result = yield self._send(message=message, **kwargs)
            return result
        raise DeclineMessage()

    def _luceneRemoteServer(self):
        return (self._host, self._port) if self._host else self.call.luceneRemoteServer()

    def _send(self, message, **kwargs):
        body = self._conversion.jsonDumpMessage(message, **kwargs)
        headers={'Content-Type': 'application/json', 'Content-Length': len(body)}
        host, port = self._luceneRemoteServer() # WARNING: can return a different server each time.
        response = yield self._httppost(host=host, port=port, request=self._path, body=body, headers=headers)
        header, responseBody = response.split(b"\r\n\r\n", 1)
        self._verify200(header, response)
        return LuceneResponse.fromJson(responseBody)

    def _httppost(self, **kwargs):
        return httppost(**kwargs)

    def _verify200(self, header, response):
        if not header.startswith(b'HTTP/1.0 200'):
            raise IOError("Expected status '200' from LuceneRemoteService, but got: " + str(response))

    _ALLOWED_METHODS = ['executeQuery', 'prefixSearch', 'fieldnames', 'drilldownFieldnames', 'executeComposedQuery', 'similarDocuments']





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

from remote import LuceneRemote
from luceneresponse import LuceneResponse
from simplejson import dumps
from socket import socket

from seecr.utils.generatorutils import returnValueFromGenerator

class SynchronousRemote(object):
    def __init__(self, **kwargs):
        self._remote = LuceneRemote(**kwargs)
        self._remote._send = self._send

    def _send(self, message, **kwargs):
        body = dumps(dict(message=message, kwargs=kwargs))
        headers={'Content-Type': 'application/json', 'Content-Length': len(body)}
        host, port = self._remote._luceneRemoteServer() # WARNING: can return a different server each time.
        response = self._httppost(host=host, port=port, request=self._remote._path, body=body, headers=headers)
        header, responseBody = response.split("\r\n\r\n", 1)
        self._remote._verify200(header, response)
        raise StopIteration(LuceneResponse.fromJson(responseBody))

    def prefixSearch(self, *args, **kwargs):
        return returnValueFromGenerator(self._remote.prefixSearch(*args, **kwargs))

    def executeQuery(self, *args, **kwargs):
        return returnValueFromGenerator(self._remote.executeQuery(*args, **kwargs))

    def _httppost(self, host, port, request, body, headers):
        sok = _socket(host, port)
        try:
            lines = [
                'POST %(request)s HTTP/1.0',
            ]
            lines += ["%s: %s" % (k, v) for k, v in headers.items()]
            lines += ['', '']
            sendBuffer = ('\r\n'.join(lines) % locals()) + body
            totalBytesSent = 0
            bytesSent = 0
            while totalBytesSent != len(sendBuffer):
                bytesSent = sok.send(sendBuffer[totalBytesSent:])
                totalBytesSent += bytesSent
            return receiveFromSocket(sok)
        finally:
            sok.close()

def _socket(host, port, timeOutInSeconds=None):
    sok = socket()
    sok.connect((host, port))
    sok.settimeout(5.0 if timeOutInSeconds is None else timeOutInSeconds)
    return sok

def receiveFromSocket(sok):
    response = ''
    part = sok.recv(1024)
    response += part
    while part != None:
        part = sok.recv(1024)
        if not part:
            break
        response += part
    return response

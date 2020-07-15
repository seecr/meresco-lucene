## begin license ##
#
# "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
#
# Copyright (C) 2013, 2015 Seecr (Seek You Too B.V.) https://seecr.nl
# Copyright (C) 2013 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
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

from remote import LuceneRemote
from meresco.core import Observable
from socket import socket
from weightless.core import retval

class SynchronousRemote(object):
    def __init__(self, **kwargs):
        self._observable = Observable()
        self._remote = LuceneRemote(**kwargs)
        self._remote._httppost = self._httppost
        self._observable.addObserver(self._remote)

    def prefixSearch(self, **kwargs):
        return retval(self._observable.any.unknown(message='prefixSearch', **kwargs))

    def fieldnames(self, **kwargs):
        return retval(self._observable.any.unknown(message='fieldnames', **kwargs))

    def drilldownFieldnames(self, **kwargs):
        return retval(self._observable.any.unknown(message='drilldownFieldnames', **kwargs))

    def executeQuery(self, *args, **kwargs):
        if len(args) == 1:
            kwargs['query'] = args[0]
        if 'cqlAbstractSyntaxTree' in kwargs:
            kwargs['query'] = kwargs.pop('cqlAbstractSyntaxTree')
        return retval(self._observable.any.unknown(message='executeQuery', **kwargs))

    def executeComposedQuery(self, *args, **kwargs):
        if len(args) == 1:
            kwargs['query'] = args[0]
        return retval(self._observable.any.unknown(message='executeComposedQuery', **kwargs))

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
            raise StopIteration(receiveFromSocket(sok))
            yield
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

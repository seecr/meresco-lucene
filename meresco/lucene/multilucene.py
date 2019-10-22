## begin license ##
#
# "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
#
# Copyright (C) 2013-2016, 2018 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2013-2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
# Copyright (C) 2015-2016 Koninklijke Bibliotheek (KB) http://www.kb.nl
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

from weightless.core import DeclineMessage
from meresco.core import Observable
from meresco.components.json import JsonDict

from _connect import _Connect
from _lucene import luceneResponseFromDict


class MultiLucene(Observable):
    def __init__(self, defaultCore, host=None, port=None):
        Observable.__init__(self)
        self._defaultCore = defaultCore
        self._host, self._port = host, port

    def initialize(self):
        yield self.all.initialize()

    def executeQuery(self, core=None, **kwargs):
        print "executeQuery", core, kwargs
        coreName = self._defaultCore if core is None else core
        response = yield self.any[coreName].executeQuery(**kwargs)
        raise StopIteration(response)

    def executeComposedQuery(self, query):
        print "executeComposedQuery", query
        for sortKey in query.sortKeys:
            coreName = sortKey.get('core', query.resultsFrom)
            self.call[coreName].updateSortKey(sortKey)
        responseDict = (yield self._connect().send(jsonDict=JsonDict(query.asDict()), path='/query/'))
        response = luceneResponseFromDict(responseDict)
        response.info = query.infoDict()
        raise StopIteration(response)
        yield

    def any_unknown(self, message, **kwargs):
        if message in ['prefixSearch', 'fieldnames', 'drilldownFieldnames', 'similarDocuments']:
            core = kwargs.get('core')
            if core is None:
                core = self._defaultCore
            result = yield self.any[core].unknown(message=message, **kwargs)
            raise StopIteration(result)
        raise DeclineMessage()

    def coreInfo(self):
        yield self.all.coreInfo()

    def _connect(self):
        host, port = (self._host, self._port) if self._host else self.call.luceneServer()
        return _Connect(host, port, observable=self)

## begin license ##
#
# "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
#
# Copyright (C) 2013, 2015 Seecr (Seek You Too B.V.) http://seecr.nl
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

from cqlparser import cql2string, parseString, CQL_QUERY
from simplejson import dumps, loads
from meresco.lucene.composedquery import ComposedQuery

class Conversion(object):
    def __init__(self):
        self._converters = []

    def _dumps_default(self, anObject):
        if isinstance(anObject, CQL_QUERY):
            return {'__CQL_QUERY__': cql2string(anObject)}
        elif isinstance(anObject, ComposedQuery):
            return {'__COMPOSED_QUERY__': dumps(anObject.asDict(), default=self._dumps_default)}
        for converter in self._converters:
            if isinstance(anObject, converter['type']):
                return {converter['name']: dumps(converter['asDict'](anObject), default=self._dumps_default)}
        raise TypeError(repr(anObject) + 'is not JSON serializable')

    def _loads_object_hook(self, dct):
        if '__CQL_QUERY__' in dct:
            return parseString(dct['__CQL_QUERY__'])
        elif '__COMPOSED_QUERY__' in dct:
            return ComposedQuery.fromDict(loads(dct['__COMPOSED_QUERY__'], object_hook=self._loads_object_hook))
        for converter in self._converters:
            if converter['name'] in dct:
                return converter['type'].fromDict(loads(dct[converter['name']]))
        return dct

    def jsonDumpMessage(self, message, **kwargs):
        return dumps(dict(message=message, kwargs=kwargs), default=self._dumps_default)

    def jsonLoadMessage(self, aString):
        result = loads(aString, object_hook=self._loads_object_hook)
        return result['message'], result['kwargs']

    def addObject(self, objectString, objectType, objectAsDict, objectFromDict):
        self._converters.append(dict(name=objectString, type=objectType, asDict=objectAsDict, fromDict=objectFromDict))
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

from cqlparser import cql2string, parseString, CQL_QUERY
from simplejson import dumps, loads

def _dumps_default(anObject):
    if isinstance(anObject, CQL_QUERY):
        return {'__CQL_QUERY__': cql2string(anObject)}
    raise TypeError(repr(anObject) + 'is not JSON serializable')

def _loads_object_hook(dct):
    if '__CQL_QUERY__' in dct:
        return parseString(dct['__CQL_QUERY__'])
    return dct

def jsonDumpMessage(message, **kwargs):
    return dumps(dict(message=message, kwargs=kwargs), default=_dumps_default)

def jsonLoadMessage(aString):
    result = loads(aString, object_hook=_loads_object_hook)
    return result['message'], result['kwargs']

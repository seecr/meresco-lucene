## begin license ##
#
# "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
#
# Copyright (C) 2015 Koninklijke Bibliotheek (KB) http://www.kb.nl
# Copyright (C) 2015-2016 Seecr (Seek You Too B.V.) http://seecr.nl
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

from struct import unpack

from simplejson import dumps, JSONEncoder, loads

from org.apache.lucene.util import OpenBitSet


def simplifiedDict(aDict):
    return loads(dumps(aDict, cls=_JsonEncoder, sort_keys=True))

class _JsonEncoder(JSONEncoder):
    def default(self, o):
        if type(o) not in [str, unicode, dict, list]:
            return str(o)
        return JSONEncoder.default(self, o)

def readOpenBitSet(data):
    numWords = unpack('>i', data[:4])[0]
    longs = []
    for i in xrange(4, len(data), 8):
        longs.append(long(unpack('>q', data[i:i+8])[0]))
    return OpenBitSet(longs, numWords)

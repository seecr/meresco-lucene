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

from org.apache.lucene.search import CachingWrapperFilter

class FilterCache(object):
    def __init__(self, compareQueryFunction, createFilterFunction, size=50):
        self._filters = []
        self._compareQueryFunction = compareQueryFunction
        self._createFilterFunction = createFilterFunction
        self._size = size

    def getFilter(self, query):
        for i, (q, f) in enumerate(self._filters):
            if self._compareQueryFunction(query, q):
                self._filters.append(self._filters.pop(i))
                return f
        f = CachingWrapperFilter(self._createFilterFunction(query))
        self._filters.append((query, f))
        if len(self._filters) > self._size:
            self._filters.pop(0)
        return f

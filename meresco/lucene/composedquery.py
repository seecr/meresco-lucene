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


class ComposedQuery(object):
    def __init__(self):
        self._coreQueries = {}
        self._resultsFrom = None
        self._matches = {}
        self._unites = []
        self._sortKeys = None
        self._stop = None
        self._start = None

    def add(self, core, query=None, facets=None, filterQueries=None):
        self._coreQueries[core] = dict(
            query=query,
            filterQueries=[] if filterQueries is None else filterQueries,
            facets=facets)

    def resultsFrom(self, core):
        self._resultsFrom = core

    def addMatch(self, **kwargs):
        if len(kwargs) != ComposedQuery.MAX_CORES:
            raise ValueError("Expected addMatch(coreA='keyA', coreB='keyB')")
        cores = sorted(kwargs.keys())
        keys = [kwargs[core] for core in cores]
        self._matches[tuple(cores)] = tuple(keys)

    def unite(self, **kwargs):
        if len(kwargs) != ComposedQuery.MAX_CORES:
            raise ValueError("Expected unite(coreA=<luceneQueryA>, coreB=<luceneQueryA>)")
        cores = sorted(kwargs.keys())
        try:
            keyNames = self.keyNames(*cores)
        except KeyError:
            raise ValueError('No match found for %s' % cores)
        for core, keyName in zip(cores, keyNames):
            self._unites.append((core, keyName, kwargs[core]))

    def unites(self):
        return self._unites

    def keyNames(self, *cores):
        try:
            return self._matches[tuple(cores)]
        except KeyError:
            return tuple(reversed(self._matches[tuple(reversed(cores))]))

    def queryFor(self, core):
        return self._coreQueries[core]['query']

    def queriesFor(self, core):
        return [q for q in [self.queryFor(core)] + self.filterQueriesFor(core) if q]

    def facetsFor(self, core):
        return self._coreQueries[core]['facets']

    def filterQueriesFor(self, core):
        return self._coreQueries[core]['filterQueries']

    @property
    def numberOfCores(self):
        return len(self._coreQueries)

    def validate(self):
        if not (1 <= self.numberOfCores <= ComposedQuery.MAX_CORES):
            raise ValueError('Unsupported number of cores, expected at most %s and at least 1.' % ComposedQuery.MAX_CORES)
        if self._resultsFrom is None:
            raise ValueError("Core for results not specified, use resultsFrom(core='core')")
        if self._resultsFrom not in self._coreQueries:
            raise ValueError("Core in resultsFrom does not match the available cores, '%s' not in %s" % (self._resultsFrom, sorted(self._coreQueries.keys())))
        if self.numberOfCores > 1:
            if len(self._matches) == 0:
                raise ValueError("No match set for cores")
            try:
                self.keyNames(*self.cores())
            except KeyError:
                raise ValueError("No match set for cores: %s" % str(self.cores()))

    def convertWith(self, convert):
        convertQuery = lambda query: (query if query is None else convert(query))
        for coreQuery in self._coreQueries.values():
            coreQuery['query'] = convertQuery(coreQuery['query'])
            coreQuery['filterQueries'] = [convertQuery(fq) for fq in coreQuery['filterQueries']]
        self._unites = [(core, keyName, convertQuery(query)) for core, keyName, query in self._unites]

    def cores(self):
        def _cores():
            yield self._resultsFrom
            for core in self._coreQueries.keys():
                if core != self._resultsFrom:
                    yield core
        return tuple(_cores())

    def otherKwargs(self):
        return dict(start=self.start, stop=self.stop, sortKeys=self.sortKeys)

    def _prop(name):
        def fget(self):
            return getattr(self, '_'+name)
        def fset(self, value):
            return setattr(self, '_'+name, value)
        return dict(fget=fget, fset=fset)
    stop = property(**_prop('stop'))
    start = property(**_prop('start'))
    sortKeys = property(**_prop('sortKeys'))

    def asDict(self):
        result = vars(self)
        result['_matches'] = dict(('->'.join(key), value) for key, value in result['_matches'].items())
        return result

    @classmethod
    def fromDict(cls, dct):
        cq = cls()
        matches = dct['_matches']
        dct['_matches'] = dict((tuple(key.split('->')), value) for key, value in matches.items())
        for attr, value in dct.items():
            setattr(cq, attr, value)
        return cq

    MAX_CORES = 2

del ComposedQuery._prop

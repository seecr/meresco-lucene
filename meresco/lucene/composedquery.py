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
    def __init__(self, resultsFromCore):
        self._resultsFrom = resultsFromCore
        self._coreQueries = {}
        self._ensureCore(resultsFromCore)
        self._matches = {}
        self._unites = []

    def setCoreQuery(self, core, query=None, facets=None, filterQueries=None):
        self._coreQueries[core] = dict(
            query=query,
            filterQueries=[] if filterQueries is None else filterQueries,
            facets=[] if facets is None else facets)

    def addFilterQuery(self, core, query):
        self._ensureCore(core)
        self._coreQueries[core]['filterQueries'].append(query)

    def addFacet(self, core, facet):
        self._ensureCore(core)
        self._coreQueries[core]['facets'].append(facet)

    def addMatch(self, coreASpec, coreBSpec):
        for coreSpec in [coreASpec, coreBSpec]:
            self._ensureCore(coreSpec['core'])
        self._matches[(coreASpec['core'], coreBSpec['core'])] = (coreASpec, coreBSpec)

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

    def isSingleCoreQuery(self):
        usedCores = set([self._resultsFrom])
        for core, coreQueryDict in self._coreQueries.items():
            if coreQueryDict.get('query') != None or \
                coreQueryDict.get('filterQueries') or \
                coreQueryDict.get('facets'):
                    usedCores.add(core)
        for core, _, _ in self._unites:
            usedCores.add(core)
        return len(usedCores) == 1

    def unites(self):
        return self._unites

    def uniteQueriesFor(self, core):
        return [uniteQuery for coreName, coreKeyName, uniteQuery in self._unites if coreName == core]

    def keyNames(self, *cores):
        coreASpec, coreBSpec = self._matchCoreSpecs(*cores)
        keyNames = []
        for coreSpec in [coreASpec, coreBSpec]:
            for keyName in ['key', 'uniqueKey']:
                if keyName in coreSpec:
                    keyNames.append(coreSpec[keyName])
                    break
            else: 
                raise KeyError('No key specificied in match for %s' % coreSpec['core'])
        return keyNames

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
        return len(self.cores())

    def cores(self):
        def _cores():
            if not self._resultsFrom is None:
                yield self._resultsFrom
            for core in self._coreQueries.keys():
                if core != self._resultsFrom:
                    yield core
        return tuple(_cores())

    def matchingCores(self):
        return set(core for coreTuple in self._matches.keys() for core in coreTuple)

    def validate(self):
        if self.numberOfCores > ComposedQuery.MAX_CORES:
            raise ValueError('Unsupported number of cores, expected at most %s.' % ComposedQuery.MAX_CORES)
        if self.numberOfCores > 1 or self._matches:
            try:
                coreASpec, coreBSpec = self._matchCoreSpecs(*self.cores())
            except KeyError:
                raise ValueError("No match set for cores %s" % str(self.cores()))
            for coreSpec in [coreASpec, coreBSpec]:
                if coreSpec['core'] == self._resultsFrom:
                    if not 'uniqueKey' in coreSpec and self.queryFor(self._resultsFrom):
                        raise ValueError("Match for result core '%s', for which one or more queries apply, must have a uniqueKey specification." % self._resultsFrom)

    def convertWith(self, convert):
        convertQuery = lambda query: (query if query is None else convert(query))
        for coreQuery in self._coreQueries.values():
            coreQuery['query'] = convertQuery(coreQuery['query'])
            coreQuery['filterQueries'] = [convertQuery(fq) for fq in coreQuery['filterQueries']]
        self._unites = [(core, keyName, convertQuery(query)) for core, keyName, query in self._unites]

    def otherKwargs(self):
        return dict(start=self.start, stop=self.stop, sortKeys=self.sortKeys, suggestionRequest=self.suggestionRequest)

    def _prop(name, defaultValue=None):
        def fget(self):
            return getattr(self, '_'+name, defaultValue)
        def fset(self, value):
            return setattr(self, '_'+name, value)
        return dict(fget=fget, fset=fset)
    stop = property(**_prop('stop'))
    start = property(**_prop('start'))
    sortKeys = property(**_prop('sortKeys'))
    suggestionRequest = property(**_prop('suggestionRequest'))

    def asDict(self):
        result = vars(self)
        result['_matches'] = dict(('->'.join(key), value) for key, value in result['_matches'].items())
        return result

    @classmethod
    def fromDict(cls, dct):
        cq = cls(dct['_resultsFrom'])
        matches = dct['_matches']
        dct['_matches'] = dict((tuple(key.split('->')), value) for key, value in matches.items())
        for attr, value in dct.items():
            setattr(cq, attr, value)
        return cq

    def _ensureCore(self, core):
        if core not in self._coreQueries:
            self.setCoreQuery(core)

    def _matchCoreSpecs(self, *cores):
        try:
            coreASpec, coreBSpec = self._matches[cores]
        except KeyError:
            coreBSpec, coreASpec = self._matches[tuple(reversed(cores))]
        return coreASpec, coreBSpec        

    MAX_CORES = 2


del ComposedQuery._prop

## begin license ##
#
# "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
#
# Copyright (C) 2013-2014 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2013-2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
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

from collections import defaultdict


class ComposedQuery(object):
    def __init__(self, resultsFromCore, query=None):
        self.cores = set()
        self._coreQueries = defaultdict(dict)
        self._matches = {}
        self._unites = []
        self.resultsFrom = resultsFromCore
        self.setCoreQuery(resultsFromCore, query=query)

    def setCoreQuery(self, core, query, filterQueries=None, facets=None):
        self.cores.add(core)
        self._coreQueries[core]['query'] = query
        if not filterQueries is None:
            for filterQuery in filterQueries:
                self.addFilterQuery(core, filterQuery)
        if not facets is None:
            for facet in facets:
                self.addFacet(core, facet)
        return self

    def addFilterQuery(self, core, query):
        self.cores.add(core)
        self._coreQueries[core].setdefault('filterQueries', []).append(query)
        return self

    def addFacet(self, core, facet):
        self.cores.add(core)
        self._coreQueries[core].setdefault('facets', []).append(facet)
        return self

    def addMatch(self, matchCoreASpec, matchCoreBSpec):
        self._matches[(matchCoreASpec['core'], matchCoreBSpec['core'])] = (matchCoreASpec, matchCoreBSpec)
        return self

    def addUnite(self, uniteCoreASpec, uniteCoreBSpec):
        if len(self.unites) > 0:
            raise ValueError("No more than 1 addUnite supported")
        coreNames = uniteCoreASpec['core'], uniteCoreBSpec['core']
        try:
            keyNames = self.keyNames(*coreNames)
            uniteCoreASpec['key'], uniteCoreBSpec['key'] = keyNames
        except KeyError:
            raise ValueError('No match found for %s' % coreNames)
        for uniteCoreSpec in (uniteCoreASpec, uniteCoreBSpec):
            self.cores.add(uniteCoreSpec['core'])
            self._unites.append(uniteCoreSpec)
        return self



    def uniteQueriesFor(self, core):
        return [d['query'] for d in self._unites if d['core'] == core]

    def keyNames(self, *cores):
        keyNames = []
        for matchCoreSpec in self._matchCoreSpecs(*cores):
            for keyName in ['key', 'uniqueKey']:
                if keyName in matchCoreSpec:
                    keyNames.append(matchCoreSpec[keyName])
                    break
            else:
                raise KeyError('No key specificied in match for %s' % matchCoreSpec['core'])
        return keyNames

    def keyName(self, core):
        # TODO: fail faster at addMatch time!
        # Note: assumes (!) for now that for a given core the key is the same in each match the core participates in.
        foundKeys = set()
        for coreTuple, matchCoreSpecTuple in self._matches.items():
            for coreName, matchCoreSpec in zip(coreTuple, matchCoreSpecTuple):
                if coreName == core:
                    foundKeys.add(matchCoreSpec.get('uniqueKey', matchCoreSpec.get('key')))
        if len(foundKeys) > 1:
            raise ValueError("Use of different keys for one core ('%s') not yet supported" % core)
        return foundKeys.pop()

    def queryFor(self, core):
        return self._getCoreSpec(core).get('query')

    def queriesFor(self, core):
        return [q for q in [self.queryFor(core)] + self.filterQueriesFor(core) if q]

    def facetsFor(self, core):
        return self._getCoreSpec(core).get('facets', [])

    def filterQueriesFor(self, core):
        return self._getCoreSpec(core).get('filterQueries', [])

    @property
    def unites(self):
        return self._unites

    @property
    def numberOfUsedCores(self):
        return len(self.cores)

    def isSingleCoreQuery(self):
        return self.numberOfUsedCores == 1

    def validate(self):
        if not self.resultsFrom in self._coreQueries:
            raise ValueError("Should provide query for core '%s'" % self.resultsFrom)
        for core in self.cores:
            if core == self.resultsFrom:
                continue
            try:
                for matchCoreSpec in self._matchCoreSpecs(self.resultsFrom, core):
                    self.keyName(matchCoreSpec['core'])  # might raise ValueError if different keys were specified for same core
                    if matchCoreSpec['core'] == self.resultsFrom:
                        if not 'uniqueKey' in matchCoreSpec:
                            raise ValueError("Match for result core '%s', for which one or more queries apply, must have a uniqueKey specification." % self.resultsFrom)
            except KeyError:
                raise ValueError("No match set for cores %s" % str((self.resultsFrom, core)))

    def convertWith(self, convert):
        convertQuery = lambda query: (query if query is None else convert(query))
        for coreQuery in self._coreQueries.values():
            if 'query' in coreQuery:
                coreQuery['query'] = convertQuery(coreQuery['query'])
            if 'filterQueries' in coreQuery:
                coreQuery['filterQueries'] = [convertQuery(fq) for fq in coreQuery['filterQueries']]
        self._unites = [dict(d, query=convertQuery(d['query'])) for d in self._unites]

    def otherKwargs(self):
        return dict(start=self.start, stop=self.stop, sortKeys=self.sortKeys, suggestionRequest=self.suggestionRequest, dedupField=self.dedupField, dedupSortField=self.dedupSortField)

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
    dedupField = property(**_prop('dedupField'))
    dedupSortField = property(**_prop('dedupSortField'))

    def asDict(self):
        result = dict(vars(self))
        result['_matches'] = dict(('->'.join(key), value) for key, value in result['_matches'].items())
        result['cores'] = list(self.cores)
        return result

    @classmethod
    def fromDict(cls, dct):
        cq = cls(dct['resultsFrom'])
        matches = dct['_matches']
        dct['_matches'] = dict((tuple(key.split('->')), value) for key, value in matches.items())
        dct['cores'] = set(dct['cores'])
        for attr, value in dct.items():
            setattr(cq, attr, value)
        return cq

    def _getCoreSpec(self, core):
        return self._coreQueries.get(core, {})

    def _matchCoreSpecs(self, *cores):
        try:
            coreASpec, coreBSpec = self._matches[cores]
        except KeyError:
            coreBSpec, coreASpec = self._matches[tuple(reversed(cores))]
        return coreASpec, coreBSpec

    def __repr__(self):
        return "%s%s" % (self.__class__.__name__, self.asDict())


del ComposedQuery._prop

## begin license ##
#
# "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
#
# Copyright (C) 2013-2015 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2013-2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
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

from collections import defaultdict
from .utils import simplifiedDict

class ComposedQuery(object):
    def __init__(self, resultsFromCore, query=None):
        self.cores = set()
        self._queries = {}
        self._filterQueries = defaultdict(list)
        self._facets = defaultdict(list)
        self._drilldownQueries = defaultdict(list)
        self._otherCoreFacetFilters = defaultdict(list)
        self._rankQueries = {}
        self._matches = {}
        self._coreKeys = {}
        self._unites = []
        self.resultsFrom = resultsFromCore
        self.setCoreQuery(resultsFromCore, query=query)

    def setCoreQuery(self, core, query, filterQueries=None, facets=None):
        self.cores.add(core)
        self._queries[core] = query
        if not filterQueries is None:
            for filterQuery in filterQueries:
                self.addFilterQuery(core, filterQuery)
        if not facets is None:
            for facet in facets:
                self.addFacet(core, facet)
        return self

    def addFilterQuery(self, core, query):
        self.cores.add(core)
        self._filterQueries[core].append(query)
        return self

    def addFacet(self, core, facet):
        self.cores.add(core)
        self._facets[core].append(facet)
        return self

    def addDrilldownQuery(self, core, drilldownQuery):
        self.cores.add(core)
        self._drilldownQueries[core].append(drilldownQuery)
        return self

    def addOtherCoreFacetFilter(self, core, query):
        self.cores.add(core)
        self._otherCoreFacetFilters[core].append(query)
        return self

    def setRankQuery(self, core, query):
        self.cores.add(core)
        self._rankQueries[core] = query
        return self

    def addMatch(self, matchCoreASpec, matchCoreBSpec):
        self._matches[(matchCoreASpec['core'], matchCoreBSpec['core'])] = (matchCoreASpec, matchCoreBSpec)
        resultsFromCoreSpecFound = False
        for matchCoreSpec in [matchCoreASpec, matchCoreBSpec]:
            coreName = matchCoreSpec['core']
            if coreName == self.resultsFrom:
                resultsFromCoreSpecFound = True
                try:
                    key = matchCoreSpec['uniqueKey']
                except KeyError:
                    raise ValueError("Match for result core '%s' must have a uniqueKey specification." % self.resultsFrom)
            else:
                key = matchCoreSpec.get('uniqueKey', matchCoreSpec.get('key'))
            if self._coreKeys.get(coreName, key) != key:
                raise ValueError("Use of different keys for one core ('%s') not yet supported" % coreName)
            self._coreKeys[coreName] = key
        if not resultsFromCoreSpecFound:
            raise ValueError("Match that does not include resultsFromCore ('%s') not yet supported" % self.resultsFrom)
        return self

    def addUnite(self, uniteCoreASpec, uniteCoreBSpec):
        if len(self.unites) > 0:
            raise ValueError("No more than 1 addUnite supported")
        for uniteCoreSpec in (uniteCoreASpec, uniteCoreBSpec):
            self.cores.add(uniteCoreSpec['core'])
            self._unites.append(uniteCoreSpec)
        return self

    def queryFor(self, core):
        return self._queries.get(core)

    def filterQueriesFor(self, core):
        return self._filterQueries.get(core, [])

    def facetsFor(self, core):
        return self._facets.get(core, [])

    def drilldownQueriesFor(self, core):
        return self._drilldownQueries.get(core, [])

    def otherCoreFacetFiltersFor(self, core):
        return self._otherCoreFacetFilters.get(core, [])

    def rankQueryFor(self, core):
        return self._rankQueries.get(core)

    def uniteQueriesFor(self, core):
        return [d['query'] for d in self._unites if d['core'] == core]

    def keyName(self, core):
        return self._coreKeys[core]

    def queriesFor(self, core):
        return [q for q in [self.queryFor(core)] + self.filterQueriesFor(core) if q]

    @property
    def unites(self):
        return [(u['core'], u['query']) for u in self._unites]

    @property
    def filterQueries(self):
        return self._filterQueries.items()

    @property
    def numberOfUsedCores(self):
        return len(self.cores)

    def isSingleCoreQuery(self):
        return self.numberOfUsedCores == 1

    def coresInMatches(self):
        return set(c for matchKey in self._matches.keys() for c in matchKey)

    def validate(self):
        for core in self.cores:
            if core == self.resultsFrom:
                continue
            try:
                self._matchCoreSpecs(self.resultsFrom, core)
            except KeyError:
                raise ValueError("No match set for cores %s" % str((self.resultsFrom, core)))

    def convertWith(self, **converts):
        convertQuery = lambda core, query: converts[core](query) if query is not None else None
        self._queries = dict((core, convertQuery(core, v)) for core, v in self._queries.items())
        self._filterQueries = dict((core, [convertQuery(core, v) for v in values]) for core, values in self._filterQueries.items())
        self._rankQueries = dict((core, convertQuery(core, v)) for core, v in self._rankQueries.items())
        self._unites = [dict(d, query=convertQuery(d['core'], d['query'])) for d in self._unites]
        self._otherCoreFacetFilters = dict((core, [convertQuery(core, v) for v in values]) for core, values in self._otherCoreFacetFilters.items())

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

    def _matchCoreSpecs(self, *cores):
        try:
            coreASpec, coreBSpec = self._matches[cores]
        except KeyError:
            coreBSpec, coreASpec = self._matches[tuple(reversed(cores))]
        return coreASpec, coreBSpec

    def __repr__(self):
        return "%s%s" % (self.__class__.__name__, self.asDict())

    def infoDict(self):
        return {
            'type': self.__class__.__name__,
            'query':simplifiedDict(dict((k.replace('_',''),v) for k,v in self.asDict().items()))
        }

del ComposedQuery._prop

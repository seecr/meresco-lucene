## begin license ##
#
# "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
#
# Copyright (C) 2013-2016 Seecr (Seek You Too B.V.) http://seecr.nl
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

from .utils import simplifiedDict


class ComposedQuery(object):
    def __init__(self, resultsFromCore, query=None):
        self.cores = set()
        self._queries = {}
        self._filterQueries = {}
        self._facets = {}
        self._drilldownQueries = {}
        self._otherCoreFacetFilters = {}
        self._rankQueries = {}
        self._matches = {}
        self._unites = []
        self._sortKeys = []
        self.resultsFrom = resultsFromCore
        if query:
            self.setCoreQuery(resultsFromCore, query=query)
        else:
            self.cores.add(resultsFromCore)


    def _makeProperty(name, defaultValue=None):
        return property(
            fget=lambda self: getattr(self, name, defaultValue),
            fset=lambda self, value: setattr(self, name, value)
        )

    stop = _makeProperty('_stop')
    start = _makeProperty('_start')
    sortKeys = _makeProperty('_sortKeys')
    suggestionRequest = _makeProperty('_suggestionRequest')
    dedupField = _makeProperty('_dedupField')
    dedupSortField = _makeProperty('_dedupSortField')
    storedFields = _makeProperty('_storedFields')
    clustering = _makeProperty('_clustering')
    clusteringConfig = _makeProperty('_clusteringConfig')
    unqualifiedTermFields = _makeProperty('_unqualifiedTermFields')
    rankQueryScoreRatio = _makeProperty('_rankQueryScoreRatio')

    del _makeProperty


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
        self._filterQueries.setdefault(core, []).append(query)
        return self

    def addFacet(self, core, facet):
        self.cores.add(core)
        self._facets.setdefault(core, []).append(facet)
        return self

    def addDrilldownQuery(self, core, drilldownQuery):
        self.cores.add(core)
        self._drilldownQueries.setdefault(core, []).append(drilldownQuery)
        return self

    def addOtherCoreFacetFilter(self, core, query):
        self.cores.add(core)
        self._otherCoreFacetFilters.setdefault(core, []).append(query)
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
                    matchCoreSpec['uniqueKey']
                except KeyError:
                    raise ValueError("Match for result core '%s' must have a uniqueKey specification." % self.resultsFrom)
        if not resultsFromCoreSpecFound:
            raise ValueError("Match that does not include resultsFromCore ('%s') not yet supported" % self.resultsFrom)
        return self

    def addUnite(self, uniteCoreASpec, uniteCoreBSpec):
        if len(self.unites) > 0:
            raise ValueError("No more than 1 addUnite supported")
        for uniteCoreSpec in (uniteCoreASpec, uniteCoreBSpec):
            self.cores.add(uniteCoreSpec['core'])
        self._unites.append(Unite(self, uniteCoreASpec, uniteCoreBSpec))
        return self

    def addSortKey(self, sortKey):
        core = sortKey.get('core', self.resultsFrom)
        self.cores.add(core)
        self._sortKeys.append(sortKey)

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

    def keyName(self, core, otherCore):
        if core == otherCore: #TODO: Needed for filters/rank's in same core as queried core
            for matchCoreASpec, matchCoreBSpec in self._matches.values():
                if matchCoreASpec['core'] == core:
                    coreSpec = matchCoreASpec
                    break
                elif matchCoreBSpec['core'] == core:
                    coreSpec = matchCoreBSpec
                    break
        else:
            coreSpec, _ = self._matchCoreSpecs(core, otherCore)
        return coreSpec.get('uniqueKey', coreSpec.get('key'))

    def keyNames(self, core):
        keyNames = set()
        for coreName in self.cores:
            if coreName != core:
                keyNames.add(self.keyName(core, coreName))
        return keyNames

    def queriesFor(self, core):
        return [q for q in [self.queryFor(core)] + self.filterQueriesFor(core) if q]

    @property
    def unites(self):
        return self._unites[:]

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
        def convertQuery(core, query):
            if query is None:
                return None
            convertFunction = converts[core]
            if core == self.resultsFrom:
                kwargs = {'composedQuery': self}
                if self.unqualifiedTermFields:
                    kwargs['unqualifiedTermFields'] = self.unqualifiedTermFields
                return convertFunction(query, **kwargs)
            return convertFunction(query)
        self._queries = dict((core, convertQuery(core, v)) for core, v in self._queries.items())
        self._filterQueries = dict((core, [convertQuery(core, v) for v in values]) for core, values in self._filterQueries.items())
        self._rankQueries = dict((core, convertQuery(core, v)) for core, v in self._rankQueries.items())
        for unite in self._unites:
            unite.convertQuery(convertQuery)
        self._otherCoreFacetFilters = dict((core, [convertQuery(core, v) for v in values]) for core, values in self._otherCoreFacetFilters.items())

    def asDict(self):
        result = dict(vars(self))
        result['_matches'] = dict(('->'.join(key), value) for key, value in result['_matches'].items())
        result['_unites'] = [unite.asDict() for unite in self._unites]
        result['cores'] = list(self.cores)
        return result

    @classmethod
    def fromDict(cls, dct):
        cq = cls(dct['resultsFrom'])
        matches = dct['_matches']
        dct['_matches'] = dict((tuple(key.split('->')), value) for key, value in matches.items())
        dct['_unites'] = [Unite.fromDict(cq, uniteDict) for uniteDict in dct['_unites']]
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
            'query': simplifiedDict(dict((k.replace('_', ''), v) for k,v in self.asDict().items()))
        }


class Unite(object):
    def __init__(self, parent, coreASpec, coreBSpec):
        self._parent = parent
        self.coreASpec = coreASpec
        self.coreBSpec = coreBSpec

    def queries(self):
        keyNameA = self._parent.keyName(self.coreASpec['core'], self.coreBSpec['core'])
        keyNameB = self._parent.keyName(self.coreBSpec['core'], self.coreASpec['core'])
        resultKeyName = keyNameA if self._parent.resultsFrom == self.coreASpec['core'] else keyNameB
        yield dict(core=self.coreASpec['core'], query=self.coreASpec['query'], keyName=keyNameA), resultKeyName
        yield dict(core=self.coreBSpec['core'], query=self.coreBSpec['query'], keyName=keyNameB), resultKeyName

    def convertQuery(self, convertQueryFunction):
        for spec in [self.coreASpec, self.coreBSpec]:
            spec['query'] = convertQueryFunction(spec['core'], spec['query'])

    def asDict(self):
        return {'A': [self.coreASpec['core'], self.coreASpec['query']], 'B': [self.coreBSpec['core'], self.coreBSpec['query']]}

    @classmethod
    def fromDict(cls, parent, dct):
        return cls(parent, dict(core=dct['A'][0], query=dct['A'][1]), dict(core=dct['B'][0], query=dct['B'][1]))

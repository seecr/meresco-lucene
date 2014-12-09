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

from org.apache.lucene.search import Sort, QueryWrapperFilter, MatchAllDocsQuery, CachingWrapperFilter, BooleanQuery, TermQuery, BooleanClause, MultiCollector, TotalHitCountCollector, TopScoreDocCollector, TopFieldCollector
from org.meresco.lucene.search import MultiSuperCollector, TopFieldSuperCollector, TotalHitCountSuperCollector, TopScoreDocSuperCollector, DeDupFilterSuperCollector, DeDupFilterCollector
from org.meresco.lucene.search.join import ScoreSuperCollector, ScoreCollector, KeySuperCollector, KeyCollector
from org.apache.lucene.index import Term
from org.apache.lucene.queries import ChainedFilter
from org.apache.lucene.search import SortField
from org.meresco.lucene.search import SuperCollector

from java.lang import Integer
from java.util import ArrayList

from time import time

from os.path import basename

from meresco.lucene.luceneresponse import LuceneResponse
from meresco.lucene.index import Index
from meresco.lucene.cache import LruCache
from meresco.lucene.fieldregistry import IDFIELD
from meresco.lucene.hit import Hit
from seecr.utils.generatorutils import generatorReturn


class Lucene(object):
    COUNT = 'count'
    SUPPORTED_SORTBY_VALUES = [COUNT]

    def __init__(self, path, reactor, fieldRegistry, commitTimeout=None, commitCount=None, name=None, multithreaded=True, **kwargs):
        self._reactor = reactor
        self._maxCommitCount = commitCount or 100000
        self._commitCount = 0
        self._commitTimeout = commitTimeout or 10
        self._commitTimerToken = None

        self._fieldRegistry = fieldRegistry
        self._multithreaded = multithreaded
        self._index = Index(path, facetsConfig=fieldRegistry.facetsConfig, multithreaded=multithreaded, **kwargs)
        self.similarityWrapper = self._index.similarityWrapper
        if name is not None:
            self.observable_name = lambda: name
        self.coreName = name or basename(path)
        self._filterCache = LruCache(
            keyEqualsFunction=lambda q1, q2: q1.equals(q2),
            createFunction=lambda q: CachingWrapperFilter(QueryWrapperFilter(q))
        )
        self._scoreCollectorCache = LruCache(
            keyEqualsFunction=lambda (k, q), (k2, q2): k == k2 and q.equals(q2),
            createFunction=lambda args: self._scoreCollector(*args)
        )
        self._collectedKeysCache = LruCache(
            keyEqualsFunction=lambda (f, k), (f1, k1): k == k1 and f.equals(f1),
            createFunction=lambda (filter, keyName): self._collectKeys(filter=filter, keyName=keyName, query=None)
        )

    def addDocument(self, identifier, document):
        document.add(self._fieldRegistry.createIdField(identifier))
        self._index.addDocument(term=Term(IDFIELD, identifier), document=document)
        self.commit()
        return
        yield

    def delete(self, identifier):
        self._index.deleteDocument(Term(IDFIELD, identifier))
        self.commit()
        return
        yield

    def commit(self):
        self._commitCount += 1
        if self._commitTimerToken is None:
            self._commitTimerToken = self._reactor.addTimer(
                    seconds=self._commitTimeout,
                    callback=lambda: self._realCommit(removeTimer=False)
                )
        if self._commitCount >= self._maxCommitCount:
            self._realCommit()
            self._commitCount = 0

    def _realCommit(self, removeTimer=True):
        self._commitTimerToken, token = None, self._commitTimerToken
        if removeTimer:
            self._reactor.removeTimer(token=token)
        self._index.commit()
        self._scoreCollectorCache.clear()
        self._collectedKeysCache.clear()

    def search(self, query=None, filterQuery=None, collector=None):
        filter_ = None
        if filterQuery:
            filter_ = QueryWrapperFilter(filterQuery)
        self._index.search(query, filter_, collector)

    def facets(self, facets, filterQueries, drilldownQueries=None, filter=None):
        facetCollector = self._facetCollector() if facets else None
        filter_ = self._filterFor(filterQueries, filter=filter)
        query = MatchAllDocsQuery()
        if drilldownQueries:
            query = self.createDrilldownQuery(query, drilldownQueries)
        self._index.search(query, filter_, facetCollector)
        generatorReturn(self._facetResult(facetCollector, facets))
        yield

    def executeQuery(self, luceneQuery, start=None, stop=None, sortKeys=None, facets=None,
            filterQueries=None, suggestionRequest=None, filter=None, dedupField=None, dedupSortField=None, scoreCollector=None, drilldownQueries=None, keyCollector=None, **kwargs):
        t0 = time()
        stop = 10 if stop is None else stop
        start = 0 if start is None else start

        collectors = []
        resultsCollector = topCollector = self._topCollector(start=start, stop=stop, sortKeys=sortKeys)
        dedupCollector = None
        if dedupField:
            constructor = DeDupFilterSuperCollector if self._multithreaded else DeDupFilterCollector
            resultsCollector = dedupCollector = constructor(dedupField, dedupSortField, topCollector)
        collectors.append(resultsCollector)

        if facets:
            facetCollector = self._facetCollector()
            collectors.append(facetCollector)
        if keyCollector:
            collectors.append(keyCollector)

        if self._multithreaded:
            multiSubCollectors = ArrayList().of_(SuperCollector)
            for c in collectors:
                multiSubCollectors.add(c)
        collector = MultiSuperCollector(multiSubCollectors) if self._multithreaded else MultiCollector.wrap(collectors)

        if scoreCollector:
            scoreCollector.setDelegate(collector)
            collector = scoreCollector

        filter_ = self._filterFor(filterQueries, filter)

        if drilldownQueries:
            luceneQuery = self.createDrilldownQuery(luceneQuery, drilldownQueries)
        self._index.search(luceneQuery, filter_, collector)

        total, hits = self._topDocsResponse(topCollector, start=start, dedupCollector=dedupCollector if dedupField else None)

        response = LuceneResponse(total=total, hits=hits, drilldownData=[])

        if dedupCollector:
            response.totalWithDuplicates = dedupCollector.totalHits

        if facets:
            response.drilldownData.extend(self._facetResult(facetCollector, facets))

        if suggestionRequest:
            response.suggestions = self._index.suggest(**suggestionRequest)

        response.queryTime = millis(time() - t0)

        raise StopIteration(response)
        yield

    def createDrilldownQuery(self, luceneQuery, drilldownQueries):
        q = BooleanQuery(True)
        if luceneQuery:
            q.add(luceneQuery, BooleanClause.Occur.MUST)
        for field, path in drilldownQueries:
            q.add(TermQuery(self._fieldRegistry.makeDrilldownTerm(field, path)), BooleanClause.Occur.MUST);
        return q

    def prefixSearch(self, fieldname, prefix, showCount=False, **kwargs):
        t0 = time()
        terms = self._index.termsForField(fieldname, prefix=prefix, **kwargs)
        hits = [((term, count) if showCount else term) for count, term in sorted(terms, reverse=True)]
        response = LuceneResponse(total=len(terms), hits=hits, queryTime=millis(time() - t0))
        raise StopIteration(response)
        yield

    def fieldnames(self, **kwargs):
        fieldnames = self._index.fieldnames()
        response = LuceneResponse(total=len(fieldnames), hits=fieldnames)
        raise StopIteration(response)
        yield

    def drilldownFieldnames(self, *args, **kwargs):
        drilldownFieldnames = self._index.drilldownFieldnames(*args, **kwargs)
        response = LuceneResponse(total=len(drilldownFieldnames), hits=drilldownFieldnames)
        raise StopIteration(response)
        yield

    def scoreCollector(self, keyName, query):
        return self._scoreCollectorCache.get((keyName, query))

    def _scoreCollector(self, keyName, query):
        scoreCollector = ScoreSuperCollector(keyName) if self._multithreaded else ScoreCollector(keyName)
        self.search(query=query, collector=scoreCollector)
        return scoreCollector

    def collectKeys(self, filter, keyName, query=None, cacheCollectedKeys=True):
        assert not (query is not None and cacheCollectedKeys), "Caching of collecting keys with queries is not allowed"
        if cacheCollectedKeys:
            return self._collectedKeysCache.get((filter, keyName))
        else:
            return self._collectKeys(filter, keyName, query=query)

    def _collectKeys(self, filter, keyName, query):
        keyCollector = KeySuperCollector(keyName) if self._multithreaded else KeyCollector(keyName)
        self.search(query=query or MatchAllDocsQuery(), filterQuery=filter, collector=keyCollector)
        return keyCollector.getCollectedKeys()

    def close(self):
        if self._commitTimerToken is not None:
            self._reactor.removeTimer(self._commitTimerToken)
        self._index.close()

    def handleShutdown(self):
        print "handle shutdown: saving Lucene core '%s'" % self.coreName
        from sys import stdout; stdout.flush()
        self.close()

    def _topDocsResponse(self, collector, start, dedupCollector=None):
        # TODO: Probably use FieldCache iso document.get()
        hits = []
        dedupCollectorFieldName = dedupCollector.getKeyName() if dedupCollector else None
        if hasattr(collector, "topDocs"):
            for scoreDoc in collector.topDocs(start).scoreDocs:
                if dedupCollector:
                    keyForDocId = dedupCollector.keyForDocId(scoreDoc.doc)
                    newDocId = keyForDocId.getDocId() if keyForDocId else scoreDoc.doc
                    hit = Hit(self._index.getDocument(newDocId).get(IDFIELD))
                    hit.duplicateCount = {dedupCollectorFieldName: keyForDocId.getCount() if keyForDocId else 0}
                else:
                    hit = Hit(self._index.getDocument(scoreDoc.doc).get(IDFIELD))
                hit.score = scoreDoc.score
                hits.append(hit)
        return collector.getTotalHits(), hits

    def _filterFor(self, filterQueries, filter=None):
        if not filterQueries:
            return filter
        filters = [self._filterCache.get(f) for f in filterQueries]
        if filter is not None:
            filters.append(filter)
        return chainFilters(filters, ChainedFilter.AND)

    def _facetResult(self, facetCollector, facets):
        facetResult = facetCollector
        if not self._multithreaded:
            facetResult = self._index.facetResult(facetCollector)
        result = []
        for f in facets:
            sortBy = f.get('sortBy')
            if not (sortBy is None or sortBy in self.SUPPORTED_SORTBY_VALUES):
                raise ValueError('Value of "sortBy" should be in %s' % self.SUPPORTED_SORTBY_VALUES)
            path = f.get('path', [])
            result.append(dict(
                    fieldname=f['fieldname'],
                    path=path,
                    terms=_termsFromFacetResult(
                            facetResult=facetResult,
                            facet=f,
                            path=path
                        )))
        return result

    def _facetCollector(self):
        return self._index.createFacetCollector()

    def coreInfo(self):
        yield self.LuceneInfo(self)

    class LuceneInfo(object):
        def __init__(inner, self):
            inner._lucene = self
            inner.name = self.coreName
            inner.numDocs = self._index.numDocs

    def _topCollector(self, start, stop, sortKeys):
        if stop <= start:
            return TotalHitCountSuperCollector() if self._multithreaded else TotalHitCountCollector()
        # fillFields = False # always true for multi-threading/sharding
        trackDocScores = True
        trackMaxScore = False
        docsScoredInOrder = True
        if sortKeys:
            sortFields = [
                self._sortField(fieldname=sortKey['sortBy'], sortDescending=sortKey['sortDescending'])
                for sortKey in sortKeys
            ]
            sort = Sort(sortFields)
        else:
            return TopScoreDocSuperCollector(stop, docsScoredInOrder) if self._multithreaded else TopScoreDocCollector.create(stop, docsScoredInOrder)
        if self._multithreaded:
            return TopFieldSuperCollector(sort, stop, trackDocScores, trackMaxScore, docsScoredInOrder)
        else:
            fillFields = False
            return TopFieldCollector.create(sort, stop, fillFields, trackDocScores, trackMaxScore, docsScoredInOrder)

    def _sortField(self, fieldname, sortDescending):
        result = SortField(fieldname, SortField.Type.STRING, sortDescending)
        result.setMissingValue(SortField.STRING_FIRST if sortDescending else SortField.STRING_LAST)
        return result

def defaults(parameter, default):
    return default if parameter is None else parameter

def chainFilters(filters, logic):
    if len(filters) == 1:
        return ChainedFilter(filters)
    elif len(filters) > 1:
        # EG: bug? in PyLucene 4.9? The wrong ctor is called if second arg is not a list.
        return ChainedFilter(filters, [logic] * len(filters))
    return None

millis = lambda seconds: int(seconds * 1000) or 1 # nobody believes less than 1 millisecs

def _termsFromFacetResult(facetResult, facet, path):
    r = facetResult.getTopChildren(facet['maxTerms'] or Integer.MAX_VALUE, facet['fieldname'], path)
    if r is None:
        return []
    terms = []
    for l in r.labelValues:
        termDict = dict(term=str(l.label), count=l.value.intValue())
        subterms = _termsFromFacetResult(facetResult, facet, path + [termDict['term']])
        if subterms:
            termDict['subterms'] = subterms
        terms.append(termDict)
    return terms


MAX_FACET_DEPTH = 10


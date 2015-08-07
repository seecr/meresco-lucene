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

from org.apache.lucene.search import Sort, QueryWrapperFilter, MatchAllDocsQuery, CachingWrapperFilter, BooleanQuery, TermQuery, BooleanClause
from org.meresco.lucene.search import MultiSuperCollector, TopFieldSuperCollector, TotalHitCountSuperCollector, TopScoreDocSuperCollector, DeDupFilterSuperCollector, GroupSuperCollector, MerescoClusterer
from org.meresco.lucene.search.join import ScoreSuperCollector, KeySuperCollector
from org.apache.lucene.index import Term
from org.apache.lucene.queries import ChainedFilter
from org.apache.lucene.search import SortField
from org.meresco.lucene.search import SuperCollector, JoinSortField

from java.lang import Integer
from java.util import ArrayList

from time import time

from os.path import basename

from meresco.core import Observable
from meresco.lucene import KEY_PREFIX
from meresco.lucene.luceneresponse import LuceneResponse
from meresco.lucene.index import Index
from meresco.lucene.cache import LruCache
from meresco.lucene.fieldregistry import IDFIELD
from meresco.lucene.hit import Hit
from seecr.utils.generatorutils import generatorReturn
from .utils import simplifiedDict


class Lucene(Observable):
    COUNT = 'count'
    SUPPORTED_SORTBY_VALUES = [COUNT]
    SORT_ON_SCORE = 'score'
    CLUSTERING_EPS = 0.4
    CLUSTERING_MIN_POINTS = 1
    CLUSTER_MORE_RECORDS = 100

    def __init__(self, path, reactor, settings, name=None, **kwargs):
        Observable.__init__(self, name=name)
        self._reactor = reactor
        self.settings = settings
        self.log = self._logMessage if settings.verbose else lambda message: None
        self._fieldRegistry = settings.fieldRegistry
        self._commitCount = 0
        self._commitTimerToken = None
        self.coreName = name or basename(path)
        self._index = Index(path, settings=settings, log=self.log, name=self.coreName, **kwargs)
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
        if settings.readonly:
            self._startCommitTimer()

        self._clusterMoreRecords = self.CLUSTER_MORE_RECORDS
        self._clusteringEps = self.CLUSTERING_EPS
        self._clusteringMinPoints = self.CLUSTERING_MIN_POINTS

    def setSettings(self, clusteringEps=None, clusteringMinPoints=None, clusterMoreRecords=None, **kwargs):
        self._clusterMoreRecords = clusterMoreRecords or self.CLUSTER_MORE_RECORDS
        self._clusteringEps = clusteringEps or self.CLUSTERING_EPS
        self._clusteringMinPoints = clusteringMinPoints or self.CLUSTERING_MIN_POINTS
        self._index.setSettings(clusterMoreRecords=clusterMoreRecords, clusteringEps=clusteringEps, **kwargs)

    def getSettings(self):
        settings = self._index.getSettings()
        settings['clusterMoreRecords'] = self._clusterMoreRecords
        settings['clusteringEps'] = self._clusteringEps
        settings['clusteringMinPoints'] = self._clusteringMinPoints
        return settings

    def addDocument(self, document, identifier=None):
        term = None
        if identifier:
            document.add(self._fieldRegistry.createIdField(identifier))
            term = Term(IDFIELD, identifier)
        self._index.addDocument(term=term, document=document)
        self.commit()
        return
        yield

    def delete(self, identifier):
        self._index.deleteDocument(Term(IDFIELD, identifier))
        self.commit()
        return
        yield

    def _startCommitTimer(self):
        self._commitTimerToken = self._reactor.addTimer(
                seconds=self.settings.commitTimeout,
                callback=lambda: self._realCommit(removeTimer=False)
            )

    def commit(self):
        self._commitCount += 1
        if self._commitTimerToken is None:
            self._startCommitTimer()
        if self._commitCount >= self.settings.commitCount:
            self._realCommit()
            self._commitCount = 0

    def _realCommit(self, removeTimer=True):
        t0 = time()
        self._commitTimerToken, token = None, self._commitTimerToken
        if removeTimer:
            self._reactor.removeTimer(token=token)
        self._index.commit()
        self._scoreCollectorCache.clear()
        self._collectedKeysCache.clear()
        if self.settings.readonly:
            self._startCommitTimer()
        self.log("[Lucene] {0}(readonly={2}): commit took: {1:.2f} seconds\n".format(self.coreName, time() - t0, self.settings.readonly))

    def search(self, query=None, filterQuery=None, collector=None):
        filter_ = None
        if filterQuery:
            filter_ = QueryWrapperFilter(filterQuery)
        self._index.search(query, filter_, collector)

    def getDocument(self, identifier):
        results = self._index.searchTopDocs(TermQuery(Term(IDFIELD, identifier)), 1)
        if results.totalHits == 0:
            return None
        return self._index.getDocument(results.scoreDocs[0].doc)

    def facets(self, facets, filterQueries, drilldownQueries=None, filters=None):
        facetCollector = self._facetCollector(facets)
        if facetCollector is None:
            raise StopIteration([])
        filter_ = self._filtersFor(filterQueries, filters=filters)
        query = MatchAllDocsQuery()
        if drilldownQueries:
            query = self.createDrilldownQuery(query, drilldownQueries)
        self._index.search(query, filter_, facetCollector)
        generatorReturn(self._facetResult(facetCollector, facets))
        yield

    def _createCollectors(self, start, stop, sortKeys=None, clusterFields=None, groupingField=None, dedupField=None, dedupSortField=None, facets=None, keyCollectors=None, scoreCollectors=None, joinSortCollectors=None, **kwargs):
        collectors = []
        dedupCollector = None
        groupingCollector = None
        facetCollector = None
        if clusterFields:
            resultsCollector = topCollector = self._topCollector(start=start, stop=stop + self._clusterMoreRecords, sortKeys=sortKeys, joinSortCollectors=joinSortCollectors)
        elif groupingField:
            topCollector = self._topCollector(start=start, stop=stop * 10, sortKeys=sortKeys, joinSortCollectors=joinSortCollectors)
            resultsCollector = groupingCollector = GroupSuperCollector(groupingField, topCollector)
        elif dedupField:
            topCollector = self._topCollector(start=start, stop=stop, sortKeys=sortKeys, joinSortCollectors=joinSortCollectors)
            constructor = DeDupFilterSuperCollector
            resultsCollector = dedupCollector = constructor(dedupField, dedupSortField, topCollector)
        else:
            resultsCollector = topCollector = self._topCollector(start=start, stop=stop, sortKeys=sortKeys, joinSortCollectors=joinSortCollectors)

        collectors.append(resultsCollector)

        facetCollector = self._facetCollector(facets)
        if facetCollector:
            collectors.append(facetCollector)
        if keyCollectors:
            collectors.extend(keyCollectors)

        multiSubCollectors = ArrayList().of_(SuperCollector)
        for c in collectors:
            multiSubCollectors.add(c)
        collector = MultiSuperCollector(multiSubCollectors)

        if scoreCollectors:
            for scoreCollector in scoreCollectors:
                scoreCollector.setDelegate(collector)
                collector = scoreCollector
        return collector, topCollector, groupingCollector, dedupCollector, facetCollector

    def executeQuery(self, luceneQuery, start=None, stop=None, facets=None, filterQueries=None, suggestionRequest=None, filters=None, drilldownQueries=None, clusterFields=None, storedFields=None, **kwargs):
        times = {}
        t0 = time()
        stop = 10 if stop is None else stop
        start = 0 if start is None else start

        topCollectorStop = stop
        while True:
            collector, topCollector, groupingCollector, dedupCollector, facetCollector = self._createCollectors(start=start, stop=topCollectorStop, facets=facets, clusterFields=clusterFields, **kwargs)
            filter_ = self._filtersFor(filterQueries, filters)

            if drilldownQueries:
                luceneQuery = self.createDrilldownQuery(luceneQuery, drilldownQueries)

            t1 = time()
            self._index.search(luceneQuery, filter_, collector)
            times['searchTime'] = millis(time() - t1)

            if clusterFields:
                t1 = time()
                total, hits = self._clusterTopDocsResponse(topCollector, start=start, stop=stop, clusterFields=clusterFields, times=times, storedFields=storedFields)
                times['totalClusterTime'] = millis(time() - t1)
            else:
                t1 = time()
                total, hits = self._topDocsResponse(topCollector, start=start, stop=stop, groupingCollector=groupingCollector, dedupCollector=dedupCollector, storedFields=storedFields)
                times['topDocsTime'] = millis(time() - t1)
            if len(hits) == stop - start or topCollectorStop >= total:
                break
            topCollectorStop *= 10

        response = LuceneResponse(total=total, hits=hits, drilldownData=[])

        if dedupCollector:
            response.totalWithDuplicates = dedupCollector.totalHits

        if facetCollector:
            t1 = time()
            response.drilldownData.extend(self._facetResult(facetCollector, facets))
            times['facetTime'] = millis(time() - t1)

        if suggestionRequest:
            t1 = time()
            response.suggestions = self._index.suggest(**suggestionRequest)
            times['suggestionTime'] = millis(time() - t1)

        response.queryTime = millis(time() - t0)
        response.times = times
        response.info = {
            'type': 'Query',
            'query': simplifiedDict(dict(
                    luceneQuery=luceneQuery,
                    start=start,
                    stop=stop,
                    facets=facets,
                    filterQueries=filterQueries,
                    suggestionRequest=suggestionRequest,
                    drilldownQueries=drilldownQueries,
                    **kwargs
                ))
            }
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
        kwargs.pop('core', None)
        drilldownFieldnames = self._index.drilldownFieldnames(*args, **kwargs)
        response = LuceneResponse(total=len(drilldownFieldnames), hits=drilldownFieldnames)
        raise StopIteration(response)
        yield

    def scoreCollector(self, keyName, query):
        return self._scoreCollectorCache.get((keyName, query))

    def _scoreCollector(self, keyName, query):
        scoreCollector = ScoreSuperCollector(keyName)
        self.search(query=query, collector=scoreCollector)
        return scoreCollector

    def collectKeys(self, filter, keyName, query=None, cacheCollectedKeys=True):
        assert not (query is not None and cacheCollectedKeys), "Caching of collecting keys with queries is not allowed"
        if cacheCollectedKeys:
            return self._collectedKeysCache.get((filter, keyName))
        else:
            return self._collectKeys(filter, keyName, query=query)

    def _collectKeys(self, filter, keyName, query):
        keyCollector = KeySuperCollector(keyName)
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

    def _topDocsResponse(self, collector, start, stop, dedupCollector=None, groupingCollector=None, storedFields=None):
        totalHits = collector.getTotalHits()
        hits = []
        dedupCollectorFieldName = dedupCollector.getKeyName() if dedupCollector else None
        groupingCollectorFieldName = groupingCollector.getKeyName() if groupingCollector else None
        seenIds = set()
        if hasattr(collector, "topDocs"):
            count = start
            for scoreDoc in collector.topDocs(start).scoreDocs:
                if count >= stop:
                    break
                if dedupCollector:
                    keyForDocId = dedupCollector.keyForDocId(scoreDoc.doc)
                    newDocId = keyForDocId.getDocId() if keyForDocId else scoreDoc.doc
                    hit = Hit(**self._storedFields(newDocId, storedFields))
                    hit.duplicateCount = {dedupCollectorFieldName: 1}
                    if keyForDocId:
                        hit.duplicateCount = {dedupCollectorFieldName: keyForDocId.getCount()}
                elif groupingCollector:
                    hit = Hit(**self._storedFields(scoreDoc.doc, storedFields))
                    if hit.id in seenIds:
                        continue
                    duplicateIds = [hit.id]
                    if totalHits > (stop - start):
                        groupedDocIds = list(groupingCollector.group(scoreDoc.doc) or [])
                        if groupedDocIds:
                            duplicateIds = [self._index.getDocument(docId).get(IDFIELD) for docId in groupedDocIds]
                    seenIds.update(set(duplicateIds))
                    hit.duplicates = {groupingCollectorFieldName: [{"id": i} for i in duplicateIds]}
                else:
                    hit = Hit(**self._storedFields(scoreDoc.doc, storedFields))
                hit.score = scoreDoc.score
                hits.append(hit)
                count += 1
        return totalHits, hits

    def _storedFields(self, docId, fields):
        doc = self._index.getDocument(docId)
        values = dict(id=doc.get(IDFIELD))
        for f in fields or []:
            values[f] = list(doc.getValues(f))
        return values

    def _interpolateEpsilon(self, hits, slice):
        eps = self._clusteringEps * (hits - slice) / self._clusterMoreRecords
        return max(min(eps, self._clusteringEps), 0.0)

    def _clusterTopDocsResponse(self, collector, start, stop, clusterFields, times, storedFields):
        totalHits = collector.getTotalHits()
        epsilon = self._interpolateEpsilon(totalHits, stop - start)
        clusterer = MerescoClusterer(self._index.getIndexReader(), epsilon, self._clusteringMinPoints)
        for fieldname, weight in clusterFields:
            clusterer.registerField(fieldname, float(weight))
        hits = []
        if hasattr(collector, "topDocs"):
            topDocs = collector.topDocs(start)
            t0 = time()
            clusterer.processTopDocs(topDocs)
            times['processTopDocsForClustering'] = millis(time() - t0)
            t0 = time()
            clusterer.finish()
            times['clusteringAlgorithm'] = millis(time() - t0)
            count = start
            seenDocIds = set()
            t0 = time()
            for scoreDoc in topDocs.scoreDocs:
                if count >= stop:
                    break
                if scoreDoc.doc in seenDocIds:
                    continue
                cluster = clusterer.cluster(scoreDoc.doc)
                topDocs = cluster.topDocs if cluster else []

                clusteredDocIds = [t.id for t in topDocs] if cluster else [scoreDoc.doc]
                seenDocIds.update(set(clusteredDocIds))

                hit = Hit(**self._storedFields(clusteredDocIds[0], storedFields))
                hit.duplicates = {
                        "topDocs": [{"id": self._index.getDocument(t.id).get(IDFIELD), "score": t.score} for t in topDocs],
                        "topTerms": [{"term": t.term, "score": t.score} for t in cluster.topTerms] if cluster else []
                    }
                hit.score = scoreDoc.score
                hits.append(hit)
                count += 1
            times['collectClusters'] = millis(time() - t0)
        return totalHits, hits

    def _filtersFor(self, filterQueries, filters=None):
        queryFilters = [self._filterCache.get(f) for f in filterQueries] if filterQueries else []
        if filters is not None:
            queryFilters.extend(filters)
        return chainFilters(queryFilters, ChainedFilter.AND)

    def _facetResult(self, facetCollector, facets):
        facetResult = facetCollector
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

    def _facetCollector(self, facets):
        if not facets:
            return None
        return self._index.createFacetCollector(fieldnames=[f['fieldname'] for f in facets])

    def coreInfo(self):
        yield self.LuceneInfo(self)

    class LuceneInfo(object):
        def __init__(inner, self):
            inner._lucene = self
            inner.name = self.coreName
            inner.numDocs = self._index.numDocs

    def _topCollector(self, start, stop, sortKeys, joinSortCollectors):
        if stop <= start:
            return TotalHitCountSuperCollector()
        # fillFields = False # always true for multi-threading/sharding
        trackDocScores = True
        trackMaxScore = False
        docsScoredInOrder = True
        if sortKeys:
            sortFields = []
            for sortKey in sortKeys:
                joinSortCollector = joinSortCollectors.get(sortKey.get('core')) if joinSortCollectors and 'core' in sortKey else None
                sortFields.append(self._sortField(fieldname=sortKey['sortBy'], sortDescending=sortKey['sortDescending'], joinSortCollector=joinSortCollector))
            sort = Sort(sortFields)
        else:
            return TopScoreDocSuperCollector(stop, docsScoredInOrder)
        return TopFieldSuperCollector(sort, stop, trackDocScores, trackMaxScore, docsScoredInOrder)

    def _sortField(self, fieldname, sortDescending, joinSortCollector):
        if fieldname == self.SORT_ON_SCORE:
            return SortField(None, SortField.Type.SCORE, not sortDescending)
        if self._fieldRegistry.pythonType(fieldname) == str:
            fieldType = SortField.Type.STRING
            missingValue = SortField.STRING_FIRST if sortDescending else SortField.STRING_LAST
        elif self._fieldRegistry.pythonType(fieldname) == int:
            fieldType = SortField.Type.INT
            missingValue = None
        else:
            raise ValueError('Unsupported sort type')
        if fieldname in ['#tags', '#ratings', '#reviews']:
            fieldType = SortField.Type.INT
            missingValue = None
        if joinSortCollector:
            field = joinSortCollector.sortField(fieldname, fieldType, sortDescending)
        else:
            field = SortField(fieldname, fieldType, sortDescending)
        field.setMissingValue(missingValue)
        return field

    def _logMessage(self, message):
        self.do.log(message=message)

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


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

from org.apache.lucene.search import MultiCollector, TopFieldCollector, Sort, QueryWrapperFilter, TotalHitCountCollector, TopScoreDocCollector, MatchAllDocsQuery, CachingWrapperFilter
from org.apache.lucene.index import Term
from org.apache.lucene.facet.search import FacetResultNode, CountFacetRequest
from org.apache.lucene.facet.taxonomy import CategoryPath
from org.apache.lucene.facet.params import FacetSearchParams
from org.apache.lucene.queries import ChainedFilter
from org.meresco.lucene import DeDupFilterCollector, ScoreCollector
from time import time

from os.path import basename

from luceneresponse import LuceneResponse
from index import Index
from cache import LruCache
from utils import IDFIELD, createIdField, sortField
from hit import Hit
from seecr.utils.generatorutils import generatorReturn


class Lucene(object):
    COUNT = 'count'
    SUPPORTED_SORTBY_VALUES = [COUNT]

    def __init__(self, path, reactor, name=None, **kwargs):
        self._index = Index(path, reactor=reactor, **kwargs)
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

    def commit(self):
        return self._index.commit()

    def addDocument(self, identifier, document, categories=None):
        document.add(createIdField(identifier))
        self._index.addDocument(term=Term(IDFIELD, identifier), document=document, categories=categories)
        self._scoreCollectorCache.clear()
        return
        yield

    def delete(self, identifier):
        self._index.deleteDocument(Term(IDFIELD, identifier))
        self._scoreCollectorCache.clear()
        return
        yield

    def search(self, query=None, filter=None, collector=None):
        self._index.search(query, filter, collector)

    def facets(self, facets, filterQueries, filter=None):
        facetCollector = self._facetCollector(facets)
        filter_ = self._filterFor(filterQueries, filter=filter)
        self._index.search(MatchAllDocsQuery(), filter_, facetCollector)
        generatorReturn(self._facetResult(facetCollector))
        yield

    def executeQuery(self, luceneQuery, start=None, stop=None, sortKeys=None, facets=None,
        filterQueries=None, suggestionRequest=None, filterCollector=None, filter=None, dedupField=None, dedupSortField=None, averageScoreCollector=None, **kwargs):
        t0 = time()
        stop = 10 if stop is None else stop
        start = 0 if start is None else start

        collectors = []
        resultsCollector = topCollector = _topCollector(start=start, stop=stop, sortKeys=sortKeys)
        dedupCollector = None
        if dedupField:
            resultsCollector = dedupCollector = DeDupFilterCollector(dedupField, dedupSortField, topCollector)
        collectors.append(resultsCollector)

        if facets:
            facetCollector = self._facetCollector(facets)
            collectors.append(facetCollector)
        collector = MultiCollector.wrap(collectors)

        if filterCollector:
            filterCollector.setDelegate(collector)
            collector = filterCollector

        if averageScoreCollector:
            averageScoreCollector.setDelegate(collector)
            collector = averageScoreCollector

        filter_ = self._filterFor(filterQueries, filter)

        self._index.search(luceneQuery, filter_, collector)

        total, hits = self._topDocsResponse(topCollector, start=start, dedupCollector=dedupCollector if dedupField else None)

        response = LuceneResponse(total=total, hits=hits, drilldownData=[])

        if dedupCollector:
            response.totalWithDuplicates = dedupCollector.totalHits

        if facets:
            response.drilldownData.extend(self._facetResult(facetCollector))

        if suggestionRequest:
            response.suggestions = self._index.suggest(**suggestionRequest)

        response.queryTime = millis(time() - t0)

        raise StopIteration(response)
        yield

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

    def scoreCollector(self, keyName, query):
        return self._scoreCollectorCache.get((keyName, query))

    def _scoreCollector(self, keyName, query):
        scoreCollector = ScoreCollector(keyName)
        self.search(query=query, collector=scoreCollector)
        return scoreCollector

    def close(self):
        self._index.close()

    def handleShutdown(self):
        print "handle shutdown: saving Lucene core '%s'" % self.coreName
        from sys import stdout; stdout.flush()
        self.close()

    def _topDocsResponse(self, collector, start, dedupCollector=None):
        # TODO: Probably use FieldCache iso document.get()
        hits = []
        if hasattr(collector, "topDocs"):
            for scoreDoc in collector.topDocs(start).scoreDocs:
                if dedupCollector:
                    keyForDocId = dedupCollector.keyForDocId(scoreDoc.doc)
                    newDocId = keyForDocId.docId if keyForDocId else scoreDoc.doc
                    hit = Hit(self._index.getDocument(newDocId).get(IDFIELD))
                    hit.duplicateCount = {dedupCollector.keyName: keyForDocId.count if keyForDocId else 0}
                else:
                    hit = Hit(self._index.getDocument(scoreDoc.doc).get(IDFIELD))
                hits.append(hit)
        return collector.getTotalHits(), hits

    def _filterFor(self, filterQueries, filter=None):
        if not filterQueries:
            return filter
        filters = [self._filterCache.get(f) for f in filterQueries]
        if filter is not None:
            filters.append(filter)
        return ChainedFilter(filters, ChainedFilter.AND)

    def _facetResult(self, facetCollector):
        def termsFromResultNode(resultNode, terms, fieldname):
            for resultNode in resultNode.subResults.iterator():
                resultNode = FacetResultNode.cast_(resultNode)
                termDict = dict(term=resultNode.label.components[-1], count=int(resultNode.value))
                pivotTerms = []
                termsFromResultNode(resultNode=resultNode, terms=pivotTerms, fieldname=fieldname)
                if pivotTerms:
                    termDict['pivot'] = dict(fieldname=fieldname, terms=pivotTerms)
                terms.append(termDict)

        facetResults = facetCollector.getFacetResults()
        if facetResults.size() == 0:
            return []
        result = []
        for facetResult in facetResults:
            resultNode = facetResult.getFacetResultNode()
            fieldname = resultNode.label.toString()
            terms = []
            termsFromResultNode(resultNode=resultNode, terms=terms, fieldname=fieldname)
            result.append(dict(fieldname=fieldname, terms=terms))
        return result

    def _facetCollector(self, facets):
        if not facets:
            return
        facetRequests = []
        for f in facets:
            sortBy = f.get('sortBy')
            if not (sortBy is None or sortBy in self.SUPPORTED_SORTBY_VALUES):
                raise ValueError('Value of "sortBy" should be in %s' % self.SUPPORTED_SORTBY_VALUES)
            facetRequest = CountFacetRequest(CategoryPath([f['fieldname']]), f['maxTerms'])
            facetRequest.setDepth(MAX_FACET_DEPTH)
            facetRequests.append(facetRequest)
        facetSearchParams = FacetSearchParams(facetRequests)
        return self._index.createFacetCollector(facetSearchParams)

    def coreInfo(self):
        yield self.LuceneInfo(self)

    class LuceneInfo(object):
        def __init__(inner, self):
            inner._lucene = self
            inner.name = self.coreName
            inner.numDocs = self._index.numDocs

def defaults(parameter, default):
    return default if parameter is None else parameter

millis = lambda seconds: int(seconds * 1000) or 1 # nobody believes less than 1 millisecs

def _topCollector(start, stop, sortKeys):
    if stop <= start:
        return TotalHitCountCollector()
    fillFields = False
    trackDocScores = True
    trackMaxScore = False
    docsScoredInOrder = True
    if sortKeys:
        sortFields = [
            sortField(fieldname=sortKey['sortBy'], sortDescending=sortKey['sortDescending'])
                for sortKey in sortKeys
        ]
        sort = Sort(sortFields)
    else:
        return TopScoreDocCollector.create(stop, docsScoredInOrder)
    return TopFieldCollector.create(sort, stop, fillFields, trackDocScores, trackMaxScore, docsScoredInOrder)


MAX_FACET_DEPTH = 10

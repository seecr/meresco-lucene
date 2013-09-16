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

from org.apache.lucene.search import MultiCollector, TopFieldCollector, Sort, QueryWrapperFilter, TotalHitCountCollector, TopScoreDocCollector
from org.apache.lucene.index import Term
from org.apache.lucene.facet.search import FacetResultNode, CountFacetRequest
from org.apache.lucene.facet.taxonomy import CategoryPath
from org.apache.lucene.facet.params import FacetSearchParams
from org.apache.lucene.queries import ChainedFilter
from time import time

from os.path import basename

from luceneresponse import LuceneResponse
from index import Index
from cache import FilterCache
from utils import IDFIELD, createIdField, sortField


class Lucene(object):
    COUNT = 'count'
    SUPPORTED_SORTBY_VALUES = [COUNT]

    def __init__(self, path, reactor, commitTimeout=None, commitCount=None, name=None):
        self._index = Index(path, reactor=reactor, commitCount=commitCount, commitTimeout=commitTimeout)
        if name is not None:
            self.observable_name = lambda: name
        self.coreName = name or basename(path)
        self._filterCache = FilterCache(
                compareQueryFunction=lambda q1, q2: q1.equals(q2),
                createFilterFunction=lambda q: QueryWrapperFilter(q)
            )

    def addDocument(self, identifier, document, categories=None):
        document.add(createIdField(identifier))
        self._index.addDocument(term=Term(IDFIELD, identifier), document=document, categories=categories)
        return
        yield

    def delete(self, identifier):
        self._index.deleteDocument(Term(IDFIELD, identifier))
        return
        yield

    def search(self, query=None, filter=None, collector=None):
        self._index.search(query, filter, collector)

    def executeQuery(self, luceneQuery, start=0, stop=10, sortKeys=None, facets=None,
        filterQueries=None, suggestionRequest=None, filterCollector=None, extraCollector=None, **kwargs):
        t0 = time()

        collectors = [extraCollector] if extraCollector else []
        topCollector = _topCollector(start=start, stop=stop, sortKeys=sortKeys)
        collectors.append(topCollector)
        if facets:
            facetCollector = self._facetCollector(facets)
            collectors.append(facetCollector)
        collector = MultiCollector.wrap(collectors)

        if filterCollector:
            filterCollector.setDelegate(collector)
            collector = filterCollector

        filter_ = None
        if filterQueries:
            filters = [self._filterCache.getFilter(f) for f in filterQueries]
            filter_ = ChainedFilter(filters, ChainedFilter.AND)

        self._index.search(luceneQuery, filter_, collector)

        total, hits = self._topDocsResponse(topCollector, start=start)
        response = LuceneResponse(total=total, hits=hits)

        if facets:
            response.drilldownData = self._facetResult(facetCollector)

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

    def finish(self):
        self._index.finish()

    def handleShutdown(self):
        print "handle shutdown: saving Lucene core '%s'" % self.coreName
        from sys import stdout; stdout.flush()
        self.finish()

    def _topDocsResponse(self, collector, start):
        # TODO: Probably use FieldCache iso document.get()
        hits = []
        if hasattr(collector, "topDocs"):
            hits = [self._index.getDocument(hit.doc).get(IDFIELD) for hit in collector.topDocs(start).scoreDocs]
        return collector.getTotalHits(), hits

    def _facetResult(self, facetCollector):
        facetResults = facetCollector.getFacetResults()
        if facetResults.size() == 0:
            return []
        result = []
        for facetResult in facetResults:
            resultNode = facetResult.getFacetResultNode()
            fieldname = resultNode.label.toString()
            terms = []
            for resultNode in resultNode.subResults.iterator():
                resultNode = FacetResultNode.cast_(resultNode)
                terms.append(dict(term=resultNode.label.components[-1], count=int(resultNode.value)))
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
            facetRequests.append(CountFacetRequest(CategoryPath([f['fieldname']]), f['maxTerms']))
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

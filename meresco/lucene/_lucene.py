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

from org.apache.lucene.search import TopScoreDocCollector, MultiCollector, TopFieldCollector, Sort, SortField
from org.apache.lucene.index import Term
from org.apache.lucene.facet.search import FacetResultNode, CountFacetRequest
from org.apache.lucene.facet.taxonomy import CategoryPath
from org.apache.lucene.facet.params import FacetSearchParams

from java.lang import Integer

from luceneresponse import LuceneResponse
from index import Index

IDFIELD = '__id__'

class Lucene(object):

    def __init__(self, path):
        self._index = Index(path)

    def addDocument(self, *args, **kwargs):
        self._index.addDocument(*args, **kwargs)
        return
        yield

    def delete(self, identifier):
        self._index.deleteDocument(Term(IDFIELD, identifier))
        return
        yield

    def executeQuery(self, luceneQuery, start=0, stop=10, sortKeys=None, facets=None, **kwargs):
        collectors = {}
        collectors['query'] = _topScoreCollector(start=start, stop=stop, sortKeys=sortKeys)
        if facets:
            collectors['facet'] = self._facetCollector(facets)

        response = self._index.search(
                lambda: self._createResponse(collectors, start=start),
                luceneQuery,
                None,
                MultiCollector.wrap(collectors.values()),
            )
        raise StopIteration(response)
        yield

    def prefixSearch(self, fieldname, prefix, limit=10):
        terms = self._index.termsForField(fieldname, prefix=prefix)
        response = LuceneResponse(total=len(terms), hits=terms)
        raise StopIteration(response)
        yield

    def _createResponse(self, collectors, start):
        total, hits = self._topDocsResponse(collectors['query'], start=start)
        response = LuceneResponse(total=total, hits=hits)
        if 'facet' in collectors:
            response.drilldownData = self._facetResult(collectors['facet'])
        return response

    def _topDocsResponse(self, collector, start):
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
            maxTerms = f.get('maxTerms', Integer.MAX_VALUE)
            facetRequests.append(CountFacetRequest(CategoryPath([f['fieldname']]), maxTerms))
        facetSearchParams = FacetSearchParams(facetRequests)
        return self._index.createFacetCollector(facetSearchParams)

def _topScoreCollector(start, stop, sortKeys):
    if sortKeys:
        sortFields = [
            SortField(sortKey['sortBy'], SortField.Type.STRING, sortKey['sortDescending'])
            for sortKey in sortKeys
        ]
        sort = Sort(sortFields)
    else:
        sort = Sort()
    fillFields = True
    trackDocScores = True
    trackMaxScore = True
    docsScoredInOrder = False
    return TopFieldCollector.create(sort, stop, fillFields, trackDocScores, trackMaxScore, docsScoredInOrder)

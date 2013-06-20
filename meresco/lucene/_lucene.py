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


from luceneresponse import LuceneResponse
from index import Index

IDFIELD = '__id__'

class Lucene(object):

    def __init__(self, path):
        self._index = Index(path)

    def addDocument(self, document):
        self._index.addDocument(document)
        return
        yield

    def executeQuery(self, luceneQuery, start=0, stop=10, sortKeys=None, **kwargs):
        collectors = {}
        collectors['query'] = _topScoreCollector(start=start, stop=stop, sortKeys=sortKeys)

        self._index.search(
                luceneQuery,
                None,
                MultiCollector.wrap([c for c in collectors.values() if c])
            )
        raise StopIteration(self._createResponse(collectors, start=start))
        yield

    def _createResponse(self, collectors, start):
        total, hits = self._topDocsResponse(collectors['query'], start=start)
        response = LuceneResponse(total=total, hits=hits)
        return response

    def _topDocsResponse(self, collector, start):
        hits = [self._index.getDocument(hit.doc).get(IDFIELD) for hit in collector.topDocs(start).scoreDocs]
        return collector.getTotalHits(), hits


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

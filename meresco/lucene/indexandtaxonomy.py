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

from org.apache.lucene.index import DirectoryReader
from org.apache.lucene.search.similarities import BM25Similarity
from org.apache.lucene.facet.taxonomy.directory import DirectoryTaxonomyReader
from org.meresco.lucene.search import SuperIndexSearcher
from java.util.concurrent import Executors


class IndexAndTaxonomy(object):

    def __init__(self, settings, indexDirectory=None, taxoDirectory=None):
        self._settings = settings
        self._similarity = settings.similarity
        self._numberOfConcurrentTasks = settings.numberOfConcurrentTasks
        self._reader = DirectoryReader.open(indexDirectory)
        self.taxoReader = DirectoryTaxonomyReader(taxoDirectory)
        self._searcher = None
        self._executor = None
        self._reopenSearcher = True

    def reopen(self):
        reader = DirectoryReader.openIfChanged(self._reader)
        if reader is None:
            return False
        self._reader.close()
        self._reader = reader
        self._reopenSearcher = True
        taxoReader = DirectoryTaxonomyReader.openIfChanged(self.taxoReader)
        if taxoReader is None:
            return True
        self.taxoReader.close()
        self.taxoReader = taxoReader
        return True

    @property
    def searcher(self):
        if not self._reopenSearcher:
            return self._searcher

        if self._executor:
            self._executor.shutdown();
        self._executor = Executors.newFixedThreadPool(self._numberOfConcurrentTasks);
        self._searcher = SuperIndexSearcher(self._reader, self._executor, self._numberOfConcurrentTasks)
        self._searcher.setSimilarity(self._similarity)
        self._reopenSearcher = False
        return self._searcher

    def setSettings(self, similarity=None, numberOfConcurrentTasks=None, **kwargs):
        # This method must be thread-safe
        if similarity is None:
            self._similarity = self._settings.similarity
        else:
            self._similarity = BM25Similarity(similarity["k1"], similarity["b"])

        if numberOfConcurrentTasks is None:
            self._numberOfConcurrentTasks = self._settings.numberOfConcurrentTasks
        else:
            self._numberOfConcurrentTasks = numberOfConcurrentTasks
        self._reopenSearcher = True

    def getSettings(self):
        return {"similarity": self.searcher.getSimilarity().toString(), "numberOfConcurrentTasks": self._numberOfConcurrentTasks}

    def close(self):
        self.taxoReader.close()
        self._reader.close()

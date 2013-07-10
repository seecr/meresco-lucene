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

from meresco.lucene import VM, createAnalyzer

from org.apache.lucene.index import IndexWriter, DirectoryReader, IndexWriterConfig, MultiFields
from org.apache.lucene.search import IndexSearcher
from org.apache.lucene.store import CompoundFileDirectory, SimpleFSDirectory, IOContext
from org.apache.lucene.util import Version
from org.apache.lucene.facet.taxonomy.directory import DirectoryTaxonomyWriter, DirectoryTaxonomyReader
from org.apache.lucene.facet.index import FacetFields
from org.apache.lucene.facet.search import FacetsCollector
from org.apache.lucene.util import BytesRef, BytesRefIterator

from java.io import File
from java.util import Arrays

from threading import Thread

from os.path import join

class Index(object):

    def __init__(self, path):
        ioContext = IOContext()
        #indexDirectory = CompoundFileDirectory(
        #        SimpleFSDirectory(File(join(path, 'index'))),
        #        "lucene", 
        #        ioContext,
        #        True)
        indexDirectory = SimpleFSDirectory(File(join(path, 'index')))
        analyzer = createAnalyzer()
        conf = IndexWriterConfig(Version.LUCENE_43, analyzer);
        self._indexWriter = IndexWriter(indexDirectory, conf)
        self._searcher = IndexSearcher(DirectoryReader.open(self._indexWriter, True))

        self._taxoDirectory = SimpleFSDirectory(File(join(path, 'taxo')))
        self._taxoWriter = DirectoryTaxonomyWriter(self._taxoDirectory)
        self._taxoWriter.commit()
        self._taxoReader = None
        self._openTaxonomyReader()
        self._committing = False

    def addDocument(self, document, categories=None):
        if categories:
            FacetFields(self._taxoWriter).addFields(document, Arrays.asList(categories))
        self._indexWriter.addDocument(document)
        self.commit()

    def deleteDocument(self, term):
        self._indexWriter.deleteDocuments(term)
        self.commit()

    def search(self, responseBuilder, *args):
        searcher = self._searcher
        indexReader = searcher.getIndexReader()
        if indexReader.tryIncRef():
            taxoReader = self._taxoReader
            if taxoReader.tryIncRef():
                try:
                    searcher.search(*args)
                    return responseBuilder()
                finally:
                    indexReader.decRef()
                    taxoReader.decRef()
            else:
                indexReader.decRef()

    def commit(self):
        reader = DirectoryReader.open(self._indexWriter, True)
        currentReader = self._searcher.getIndexReader()
        if reader != currentReader:
            self._searcher = IndexSearcher(reader)
            currentReader.decRef()
        #thread = Thread(target=self._thread, kwargs={'target': self._hardCommit})
        #thread.start()

    def _thread(self, target):
        VM.attachCurrentThread()
        target()

    def termsForField(self, field, prefix=None):
        indexReader = self._searcher.getIndexReader()
        if indexReader.tryIncRef():
            terms = []
            try:
                iterator = MultiFields.getFields(indexReader).terms(field).iterator(None)
                if prefix:
                    iterator.seekCeil(BytesRef(prefix))
                    terms.append(iterator.term().utf8ToString())
                iterator = BytesRefIterator.cast_(iterator)
                try:
                    while True:
                        term = iterator.next().utf8ToString()
                        if prefix and not term.startswith(prefix):
                            break
                        terms.append(term)
                except StopIteration:
                    pass
                return terms
            finally:
                indexReader.decRef()

    def _hardCommit(self):
        print '_hardCommit', self
        if self._committing:
            return
        self._committing = True
        def _commit():
            self._indexWriter.commit()
            self._commitFacet()
        _commit()
        self._committing = False

    def _commitFacet(self):
        self._taxoWriter.commit()
        self._openTaxonomyReader()

    def getDocument(self, docId):
        return self._searcher.doc(docId)

    def _openTaxonomyReader(self):
        taxonomyReader = DirectoryTaxonomyReader(self._taxoDirectory)
        if taxonomyReader != self._taxoReader:
            oldTaxonomyReader = self._taxoReader
            self._taxoReader = taxonomyReader
            if oldTaxonomyReader:
                oldTaxonomyReader.decRef()

    def createFacetCollector(self, facetSearchParams):
        if not self._taxoReader:
            return
        return FacetsCollector.create(facetSearchParams, self._searcher.getIndexReader(), self._taxoReader)

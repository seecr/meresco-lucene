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

from org.apache.lucene.index import IndexWriter, DirectoryReader, IndexWriterConfig, MultiFields, Term
from org.apache.lucene.search import IndexSearcher
from org.apache.lucene.store import SimpleFSDirectory
from org.apache.lucene.util import Version
from org.apache.lucene.facet.taxonomy.directory import DirectoryTaxonomyWriter, DirectoryTaxonomyReader
from org.apache.lucene.facet.index import FacetFields
from org.apache.lucene.facet.search import FacetsCollector
from org.apache.lucene.util import BytesRef, BytesRefIterator
from org.apache.lucene.search.spell import DirectSpellChecker
from org.apache.lucene.analysis.tokenattributes import CharTermAttribute, OffsetAttribute

from java.io import File, StringReader
from java.util import Arrays

from threading import Thread

from os.path import join

# Facet documentation: http://lucene.apache.org/core/4_3_0/facet/org/apache/lucene/facet/doc-files/userguide.html#concurrent_indexing_search

class Index(object):
    def __init__(self, path):
        # ioContext = IOContext()
        #indexDirectory = CompoundFileDirectory(
        #        SimpleFSDirectory(File(join(path, 'index'))),
        #        "lucene", 
        #        ioContext,
        #        True)
        indexDirectory = SimpleFSDirectory(File(join(path, 'index')))
        self._analyzer = createAnalyzer()
        conf = IndexWriterConfig(Version.LUCENE_43, self._analyzer);
        self._indexWriter = IndexWriter(indexDirectory, conf)
        self._searcher = IndexSearcher(DirectoryReader.open(self._indexWriter, True))
        self._checker = DirectSpellChecker()


        self._taxoDirectory = SimpleFSDirectory(File(join(path, 'taxo')))
        self._taxoWriter = DirectoryTaxonomyWriter(self._taxoDirectory)
        self._taxoWriter.commit()
        self._taxoReader = None
        self._openTaxonomyReader()
        self._thread = None
        self._dirty = False

    def addDocument(self, term, document, categories=None):
        if categories:
            FacetFields(self._taxoWriter).addFields(document, Arrays.asList(categories))
        self._indexWriter.updateDocument(term, document)
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
                    taxoReader.decRef()
                    indexReader.decRef()
            else:
                indexReader.decRef()

    def suggest(self, query, count, field):
        suggestions = {}
        for token, startOffset, endOffset in self._analyzeToken(query):
            suggestWords = self._checker.suggestSimilar(Term(field, token), count, self._searcher.getIndexReader())
            if suggestWords:
                suggestions[token] = (startOffset, endOffset, [suggestWord.string for suggestWord in suggestWords])
        return suggestions

    def commit(self):
        currentReader = self._searcher.getIndexReader()
        reader = DirectoryReader.openIfChanged(currentReader)
        if reader is not None:
            self._searcher = IndexSearcher(reader)
            currentReader.decRef()
        if self._thread:
            self._dirty = True
            return
        self._thread = Thread(target=self._runThread, kwargs={'target': self._hardCommit})
        self._thread.start()

    def finish(self):
        try:
            self._thread.join()
        except AttributeError:
            pass

    def _runThread(self, target):
        VM.attachCurrentThread()
        target()

    def termsForField(self, field, prefix=None):
        indexReader = self._searcher.getIndexReader()
        if indexReader.tryIncRef():
            terms = []
            try:
                fields = MultiFields.getFields(indexReader)
                if fields is None:
                    return terms
                iterator = fields.terms(field).iterator(None)
                if prefix:
                    iterator.seekCeil(BytesRef(prefix))
                    terms.append((iterator.docFreq(), iterator.term().utf8ToString()))
                bytesIterator = BytesRefIterator.cast_(iterator)
                try:
                    while True:
                        term = bytesIterator.next().utf8ToString()
                        if prefix and not term.startswith(prefix):
                            break
                        terms.append((iterator.docFreq(), term))
                except StopIteration:
                    pass
                return terms
            finally:
                indexReader.decRef()

    def _hardCommit(self):
        def _commit():
            self._dirty = False
            self._commitFacet()
            self._indexWriter.commit()
            if self._dirty:
                _commit()
        _commit()
        self._thread = None

    def _commitFacet(self):
        self._taxoWriter.commit()
        self._openTaxonomyReader()

    def getDocument(self, docId):
        return self._searcher.doc(docId)

    def _openTaxonomyReader(self):
        if self._taxoReader:
            taxonomyReader = DirectoryTaxonomyReader.openIfChanged(self._taxoReader)
        else:
            taxonomyReader = DirectoryTaxonomyReader(self._taxoDirectory)
        if taxonomyReader is not None:
            oldTaxonomyReader = self._taxoReader
            self._taxoReader = taxonomyReader
            if oldTaxonomyReader:
                oldTaxonomyReader.decRef()

    def createFacetCollector(self, facetSearchParams):
        if not self._taxoReader:
            return
        return FacetsCollector.create(facetSearchParams, self._searcher.getIndexReader(), self._taxoReader)

    def _analyzeToken(self, token):
        result = []
        reader = StringReader(unicode(token))
        stda = self._analyzer
        ts = stda.tokenStream("dummy field name", reader)
        termAtt = ts.addAttribute(CharTermAttribute.class_)
        offsetAtt = ts.addAttribute(OffsetAttribute.class_)
        try:
            ts.reset()
            while ts.incrementToken():
                result.append((termAtt.toString(), offsetAtt.startOffset(), offsetAtt.endOffset()))
            ts.end()
        finally:
            ts.close()
        return result
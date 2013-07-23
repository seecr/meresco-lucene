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

from indexandtaxonomy import IndexAndTaxonomy

# Facet documentation: http://lucene.apache.org/core/4_3_0/facet/org/apache/lucene/facet/doc-files/userguide.html

class Index(object):
    def __init__(self, path):
        # ioContext = IOContext()
        #indexDirectory = CompoundFileDirectory(
        #        SimpleFSDirectory(File(join(path, 'index'))),
        #        "lucene", 
        #        ioContext,
        #        True)
        self._checker = DirectSpellChecker()
        indexDirectory = SimpleFSDirectory(File(join(path, 'index')))
        self._taxoDirectory = SimpleFSDirectory(File(join(path, 'taxo')))
        self._analyzer = createAnalyzer()
        conf = IndexWriterConfig(Version.LUCENE_43, self._analyzer);
        self._indexWriter = IndexWriter(indexDirectory, conf)
        self._taxoWriter = DirectoryTaxonomyWriter(self._taxoDirectory)
        self._taxoWriter.commit()

        self._indexAndTaxonomy = IndexAndTaxonomy(self._indexWriter, self._taxoDirectory)
        self._thread = None

    def addDocument(self, term, document, categories=None):
        if categories:
            FacetFields(self._taxoWriter).addFields(document, Arrays.asList(categories))
        self._indexWriter.updateDocument(term, document)
        self.commit()

    def deleteDocument(self, term):
        self._indexWriter.deleteDocuments(term)
        self.commit()

    def search(self, responseBuilder, *args):
        indexAndTaxonomy = self._getIndexAndTaxonomyAndIncRef()
        try:
            indexAndTaxonomy.searcher.search(*args)
            return responseBuilder()
        finally:
            indexAndTaxonomy.decRef()

    def suggest(self, query, count, field):
        suggestions = {}
        indexAndTaxonomy = self._getIndexAndTaxonomyAndIncRef()
        try:
            for token, startOffset, endOffset in self._analyzeToken(query):
                suggestWords = self._checker.suggestSimilar(Term(field, token), count, indexAndTaxonomy.searcher.getIndexReader())
                if suggestWords:
                    suggestions[token] = (startOffset, endOffset, [suggestWord.string for suggestWord in suggestWords])
        finally:
            indexAndTaxonomy.decRef()
        return suggestions

    def finish(self):
        try:
            self._thread.join()
        except AttributeError:
            pass

    def termsForField(self, field, prefix=None, limit=None):
        limit = 10 if limit is None else limit
        indexAndTaxonomy = self._getIndexAndTaxonomyAndIncRef()
        terms = []
        try:
            fields = MultiFields.getFields(indexAndTaxonomy.searcher.getIndexReader())
            if fields is None:
                return terms
            iterator = fields.terms(field).iterator(None)
            if prefix:
                iterator.seekCeil(BytesRef(prefix))
                terms.append((iterator.docFreq(), iterator.term().utf8ToString()))
            bytesIterator = BytesRefIterator.cast_(iterator)
            try:
                while len(terms) < limit:
                    term = bytesIterator.next().utf8ToString()
                    if prefix and not term.startswith(prefix):
                        break
                    terms.append((iterator.docFreq(), term))
            except StopIteration:
                pass
            return terms
        finally:
            indexAndTaxonomy.decRef()

    def fieldnames(self):
        indexAndTaxonomy = self._getIndexAndTaxonomyAndIncRef()
        fieldnames = []
        try:
            fields = MultiFields.getFields(indexAndTaxonomy.searcher.getIndexReader())
            if fields is None:
                return fieldnames
            iterator = fields.iterator()
            while iterator.hasNext():
                fieldnames.append(iterator.next())
            return fieldnames
        finally:
            indexAndTaxonomy.decRef()

    def commit(self):
        if self._thread:
            self._dirty = True
            return
        self._thread = Thread(target=self._runThread, kwargs={'target': self._hardCommit})
        self._thread.start()

    def _runThread(self, target):
        VM.attachCurrentThread()
        target()

    def _hardCommit(self):
        def _commit():
            self._dirty = False
            self._taxoWriter.commit()
            self._indexWriter.commit()
            if self._dirty:
                _commit()
        _commit()
        indexAndTaxonomy = self._indexAndTaxonomy.refreshIfNeeded()
        if indexAndTaxonomy:
            self._indexAndTaxonomy = indexAndTaxonomy
        self._thread = None

    def _getIndexAndTaxonomyAndIncRef(self):
        while True:
            indexAndTaxonomy = self._indexAndTaxonomy
            if indexAndTaxonomy.tryIncRef():
                return indexAndTaxonomy



    def getDocument(self, docId):
        indexAndTaxonomy = self._getIndexAndTaxonomyAndIncRef()
        try:
            return self._indexAndTaxonomy.searcher.doc(docId)
        except:
            indexAndTaxonomy.decRef()

    def createFacetCollector(self, facetSearchParams):
        indexAndTaxonomy = self._getIndexAndTaxonomyAndIncRef()
        try:
            return FacetsCollector.create(facetSearchParams, indexAndTaxonomy.searcher.getIndexReader(), indexAndTaxonomy.taxoReader)
        except:
            indexAndTaxonomy.decRef()

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
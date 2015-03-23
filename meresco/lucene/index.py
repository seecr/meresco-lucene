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

from indexandtaxonomy import IndexAndTaxonomy
from os.path import join

from java.io import File, StringReader
from org.apache.lucene.analysis.tokenattributes import CharTermAttribute, OffsetAttribute
from org.apache.lucene.facet import FacetsCollector, Facets
from org.apache.lucene.facet.taxonomy import CachedOrdinalsReader, DocValuesOrdinalsReader, TaxonomyFacetCounts, TaxonomyReader
from org.apache.lucene.facet.taxonomy.directory import DirectoryTaxonomyWriter
from org.apache.lucene.facet.taxonomy.writercache import LruTaxonomyWriterCache
from org.apache.lucene.index import IndexWriter, IndexWriterConfig, MultiFields, Term
from org.apache.lucene.index import TieredMergePolicy
from org.apache.lucene.search.spell import DirectSpellChecker
from org.apache.lucene.store import MMapDirectory
from org.apache.lucene.util import BytesRef, BytesRefIterator
from org.apache.lucene.util import Version
from org.meresco.lucene.search import FacetSuperCollector


class Index(object):
    def __init__(self, path, settings):
        self._settings = settings
        self._checker = DirectSpellChecker()
        indexDirectory = MMapDirectory(File(join(path, 'index')))
        indexDirectory.setUseUnmap(False)
        taxoDirectory = MMapDirectory(File(join(path, 'taxo')))
        taxoDirectory.setUseUnmap(False)
        conf = IndexWriterConfig(Version.LUCENE_4_10_0, settings.analyzer)
        conf.setSimilarity(settings.similarity)
        mergePolicy = TieredMergePolicy()
        mergePolicy.setMaxMergeAtOnce(settings.maxMergeAtOnce)
        mergePolicy.setSegmentsPerTier(settings.segmentsPerTier)
        conf.setMergePolicy(mergePolicy)

        if not settings.readonly:
            self._indexWriter = IndexWriter(indexDirectory, conf)
            self._indexWriter.commit()
            self._taxoWriter = DirectoryTaxonomyWriter(taxoDirectory, IndexWriterConfig.OpenMode.CREATE_OR_APPEND, LruTaxonomyWriterCache(settings.lruTaxonomyWriterCacheSize))
            self._taxoWriter.commit()

        self._indexAndTaxonomy = IndexAndTaxonomy(settings, indexDirectory, taxoDirectory)
        self._readerSettingsWrapper = self._indexAndTaxonomy._readerSettingsWrapper

        self._facetsConfig = settings.fieldRegistry.facetsConfig

        self._ordinalsReader = CachedOrdinalsReader(DocValuesOrdinalsReader())

    def addDocument(self, document, term=None):
        document = self._facetsConfig.build(self._taxoWriter, document)
        if term is None:
            self._indexWriter.addDocument(document)
        else:
            self._indexWriter.updateDocument(term, document)

    def deleteDocument(self, term):
        self._indexWriter.deleteDocuments(term)

    def search(self, query, filter, collector):
        self._indexAndTaxonomy.searcher.search(query, filter, collector)

    def suggest(self, query, count, field):
        suggestions = {}
        for token, startOffset, endOffset in self._analyzeToken(query):
            suggestWords = self._checker.suggestSimilar(Term(field, token), count, self._indexAndTaxonomy.searcher.getIndexReader())
            if suggestWords:
                suggestions[token] = (startOffset, endOffset, [suggestWord.string for suggestWord in suggestWords])
        return suggestions

    def termsForField(self, field, prefix=None, limit=10, **kwargs):
        convert = lambda term: term.utf8ToString()
        terms = []
        termsEnum = MultiFields.getTerms(self._indexAndTaxonomy.searcher.getIndexReader(), field)
        if termsEnum is None:
            return terms
        iterator = termsEnum.iterator(None)
        if prefix:
            iterator.seekCeil(BytesRef(prefix))
            terms.append((iterator.docFreq(), convert(iterator.term())))
        bytesIterator = BytesRefIterator.cast_(iterator)
        try:
            while len(terms) < limit:
                term = convert(bytesIterator.next())
                if prefix and not term.startswith(prefix):
                    break
                terms.append((iterator.docFreq(), term))
        except StopIteration:
            pass
        return terms

    def fieldnames(self):
        indexAndTaxonomy = self._indexAndTaxonomy
        fieldnames = []
        fields = MultiFields.getFields(indexAndTaxonomy.searcher.getIndexReader())
        if fields is None:
            return fieldnames
        iterator = fields.iterator()
        while iterator.hasNext():
            fieldnames.append(iterator.next())
        return fieldnames

    def drilldownFieldnames(self, path=None, limit=50):
        taxoReader = self._indexAndTaxonomy.taxoReader
        parentOrdinal = TaxonomyReader.ROOT_ORDINAL if path is None else taxoReader.getOrdinal(path[0], path[1:])
        childrenIter = taxoReader.getChildren(parentOrdinal)
        names = []
        while True:
            ordinal = childrenIter.next()
            if ordinal == TaxonomyReader.INVALID_ORDINAL:
                break
            names.append(taxoReader.getPath(ordinal).components[-1])
            if len(names) >= limit:
                break
        return names

    def numDocs(self):
        return self._indexAndTaxonomy.searcher.getIndexReader().numDocs()

    def commit(self):
        if not self._settings.readonly:
            self._taxoWriter.commit()
            self._indexWriter.commit()
        self._indexAndTaxonomy.reopen()

    def getDocument(self, docId):
        return self._indexAndTaxonomy.searcher.doc(docId)

    def createFacetCollector(self):
        return FacetSuperCollector(self._indexAndTaxonomy.taxoReader, self._facetsConfig, self._ordinalsReader)

    def facetResult(self, facetCollector):
        facetResult = TaxonomyFacetCounts(self._ordinalsReader, self._indexAndTaxonomy.taxoReader, self._facetsConfig, facetCollector)
        return Facets.cast_(facetResult)

    def close(self):
        self._indexAndTaxonomy.close()
        if not self._settings.readonly:
            self._taxoWriter.close()
            self._indexWriter.close()

    def _analyzeToken(self, token):
        result = []
        reader = StringReader(unicode(token))
        stda = self._settings.analyzer
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


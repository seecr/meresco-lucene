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

from meresco.lucene import createAnalyzer

from org.apache.lucene.index import IndexWriter, IndexWriterConfig, MultiFields, Term
from org.apache.lucene.store import SimpleFSDirectory
from org.apache.lucene.util import Version
from org.apache.lucene.facet.taxonomy.directory import DirectoryTaxonomyWriter
from org.apache.lucene.facet import FacetsCollector, FacetsConfig, Facets
from org.apache.lucene.util import BytesRef, BytesRefIterator, NumericUtils
from org.apache.lucene.search.spell import DirectSpellChecker
from org.apache.lucene.search.similarities import BM25Similarity
from org.apache.lucene.analysis.tokenattributes import CharTermAttribute, OffsetAttribute
from org.apache.lucene.facet.taxonomy.writercache import LruTaxonomyWriterCache
from org.meresco.lucene.search import FacetSuperCollector
from java.io import File, StringReader

from os.path import join

from indexandtaxonomy import IndexAndTaxonomy
from meresco.lucene.utils import fieldType, LONGTYPE
from org.apache.lucene.facet.taxonomy import TaxonomyFacetCounts, CachedOrdinalsReader, DocValuesOrdinalsReader

class Index(object):
    def __init__(self, path, reactor, commitTimeout=None, commitCount=None, lruTaxonomyWriterCacheSize=4000, analyzer=None, similarity=None, facetsConfig=None, drilldownFields=None, executor=None):
        self._reactor = reactor
        self._maxCommitCount = commitCount or 1000
        self._commitCount = 0
        self._commitTimeout = commitTimeout or 1
        self._commitTimerToken = None
        similarity = similarity or BM25Similarity()

        self._checker = DirectSpellChecker()
        indexDirectory = SimpleFSDirectory(File(join(path, 'index')))
        self._taxoDirectory = SimpleFSDirectory(File(join(path, 'taxo')))
        self._analyzer = createAnalyzer(analyzer=analyzer)
        conf = IndexWriterConfig(Version.LUCENE_48, self._analyzer)
        conf.setSimilarity(similarity)
        self._indexWriter = IndexWriter(indexDirectory, conf)
        self._taxoWriter = DirectoryTaxonomyWriter(self._taxoDirectory, IndexWriterConfig.OpenMode.CREATE_OR_APPEND, LruTaxonomyWriterCache(lruTaxonomyWriterCacheSize))
        self._taxoWriter.commit()

        self._indexAndTaxonomy = IndexAndTaxonomy(self._indexWriter, self._taxoWriter, similarity, executor)
        self.similarityWrapper = self._indexAndTaxonomy.similarityWrapper

        self._facetsConfig = facetsConfig

        self._ordinalsReader = CachedOrdinalsReader(DocValuesOrdinalsReader())

    def addDocument(self, term, document):
        document = self._facetsConfig.build(self._taxoWriter, document)
        self._indexWriter.updateDocument(term, document)
        self.commit()

    def deleteDocument(self, term):
        self._indexWriter.deleteDocuments(term)
        self.commit()

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
        if fieldType(field) == LONGTYPE:
            convert = lambda term: NumericUtils.prefixCodedToLong(term)
            if prefix:
                raise ValueError('No prefixSearch for number fields.')
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

    def numDocs(self):
        return self._indexAndTaxonomy.searcher.getIndexReader().numDocs()

    def commit(self):
        self._commitCount += 1
        if self._commitTimerToken is None:
            self._commitTimerToken = self._reactor.addTimer(
                    seconds=self._commitTimeout,
                    callback=lambda: self._realCommit(removeTimer=False)
                )
        if self._commitCount >= self._maxCommitCount:
            self._realCommit()
            self._commitCount = 0

    def _realCommit(self, removeTimer=True):
        self._commitTimerToken, token = None, self._commitTimerToken
        if removeTimer:
            self._reactor.removeTimer(token=token)
        self._taxoWriter.commit()
        self._indexWriter.commit()
        self._indexAndTaxonomy.reopen()

    def getDocument(self, docId):
        return self._indexAndTaxonomy.searcher.doc(docId)

    def createFacetCollector(self):
        return FacetSuperCollector(self._indexAndTaxonomy.taxoReader, self._facetsConfig)

    def facetResult(self, facetCollector):
        facetResult = TaxonomyFacetCounts(self._ordinalsReader, self._indexAndTaxonomy.taxoReader, self._facetsConfig, facetCollector)
        return Facets.cast_(facetResult)

    def close(self):
        if self._commitTimerToken is not None:
            self._reactor.removeTimer(self._commitTimerToken)
        self._indexAndTaxonomy.close()
        self._taxoWriter.close()
        self._indexWriter.close()

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


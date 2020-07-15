## begin license ##
#
# "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
#
# Copyright (C) 2014-2017, 2019 Seecr (Seek You Too B.V.) https://seecr.nl
# Copyright (C) 2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
# Copyright (C) 2015-2016 Koninklijke Bibliotheek (KB) http://www.kb.nl
# Copyright (C) 2016 Stichting Kennisnet http://www.kennisnet.nl
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

imported = False
def lazyImport():
    global imported
    if imported:
        return

    from meresco.pylucene import getJVM
    getJVM()

    from java.nio.file import Paths
    from org.apache.lucene.document import Document, StringField, Field, FieldType
    from org.apache.lucene.search import IndexSearcher, TermQuery
    from org.apache.lucene.index import DirectoryReader, Term, IndexWriter, IndexWriterConfig, IndexOptions
    from org.apache.lucene.store import FSDirectory
    from org.apache.lucene.util import Version
    from org.apache.lucene.analysis.core import WhitespaceAnalyzer

    UNINDEXED_TYPE = FieldType()
    UNINDEXED_TYPE.setIndexOptions(IndexOptions.NONE)
    UNINDEXED_TYPE.setStored(True)
    UNINDEXED_TYPE.setTokenized(False)

    imported = True
    globals().update(locals())


class LuceneKeyValueStore(object):
    def __init__(self, path):
        lazyImport()
        self._writer, self._reader, self._searcher = self._getLucene(path)
        self._latestModifications = {}
        self._doc = Document()
        self._keyField = StringField("key", "", Field.Store.NO)
        self._valueField = Field("value", "", UNINDEXED_TYPE)
        self._doc.add(self._keyField)
        self._doc.add(self._valueField)

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def __setitem__(self, key, value):
        key = str(key)
        value = str(value)
        self._maybeReopen()
        self._keyField.setStringValue(key)
        self._valueField.setStringValue(value)
        self._writer.updateDocument(Term("key", key), self._doc)
        self._latestModifications[key] = value

    def __getitem__(self, key):
        key = str(key)
        value = self._latestModifications.get(key)
        if value is DELETED_RECORD:
            raise KeyError(key)
        if not value is None:
            return value
        self._maybeReopen()
        topDocs = self._searcher.search(TermQuery(Term("key", key)), 1)
        if topDocs.totalHits.value == 0:
            raise KeyError(key)
        return self._searcher.doc(topDocs.scoreDocs[0].doc).get("value")

    def __delitem__(self, key):
        key = str(key)
        self._writer.deleteDocuments(Term("key", key))
        self._latestModifications[key] = DELETED_RECORD

    def __len__(self):
        raise NotImplementedError

    def __iter__(self):
        raise NotImplementedError

    def items(self):
        raise NotImplementedError

    def keys(self):
        raise NotImplementedError

    def values(self):
        raise NotImplementedError

    def _getLucene(self, path):
        directory = FSDirectory.open(Paths.get(path))
        config = IndexWriterConfig(None)
        config.setRAMBufferSizeMB(256.0) # faster
        config.setUseCompoundFile(False) # faster, for Lucene 4.4 and later
        writer = IndexWriter(directory, config)
        reader = writer.getReader()
        searcher = IndexSearcher(reader)
        return writer, reader, searcher

    def _maybeReopen(self):
        if len(self._latestModifications) > 10000:
            newReader = DirectoryReader.openIfChanged(self._reader, self._writer, True)
            if not newReader is None:
                self._reader.close()
                self._reader = newReader
                self._searcher = IndexSearcher(self._reader)
                self._latestModifications.clear()

    def commit(self):
        self._writer.commit()

    def close(self):
        self._writer.close()


DELETED_RECORD = object()

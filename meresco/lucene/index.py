
from org.apache.lucene.index import IndexWriter, DirectoryReader, IndexWriterConfig
from org.apache.lucene.search import IndexSearcher
from org.apache.lucene.store import SimpleFSDirectory
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.util import Version

from java.io import File

from os.path import join

class Index(object):

    def __init__(self, path):
        indexDirectory = SimpleFSDirectory(File(join(path, 'index')))
        analyzer = StandardAnalyzer(Version.LUCENE_43)
        conf = IndexWriterConfig(Version.LUCENE_43, analyzer);
        self._indexWriter = IndexWriter(indexDirectory, conf)
        self._searcher = IndexSearcher(DirectoryReader.open(self._indexWriter, True))

    def addDocument(self, document):
        self._indexWriter.addDocument(document)
        self.commit()

    def search(self, *args):
        searcher = self._searcher
        indexReader = searcher.getIndexReader()
        if indexReader.tryIncRef():
            try:
                searcher.search(*args)
            finally:
                indexReader.decRef()

    def commit(self):
        reader = DirectoryReader.open(self._indexWriter, True)
        currentReader = self._searcher.getIndexReader()
        if reader != currentReader:
            self._searcher = IndexSearcher(reader)
            currentReader.decRef()

    def getDocument(self, docId):
        return self._searcher.doc(docId)

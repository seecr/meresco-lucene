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

from org.apache.lucene.store import MMapDirectory
from org.apache.lucene.facet.taxonomy.directory import DirectoryTaxonomyWriter, DirectoryTaxonomyReader
from org.apache.lucene.facet.taxonomy import FacetLabel
from org.apache.lucene.index import IndexWriterConfig
from org.apache.lucene.facet.taxonomy.writercache import LruTaxonomyWriterCache
from java.io import File

from meresco.core import Observable

class TermNumerator(Observable):

    def __init__(self, path, lruTaxonomyWriterCacheSize=100):
        Observable.__init__(self)
        taxoDirectory = MMapDirectory(File(path))
        taxoDirectory.setUseUnmap(False)
        self._taxoWriter = DirectoryTaxonomyWriter(taxoDirectory, IndexWriterConfig.OpenMode.CREATE_OR_APPEND, LruTaxonomyWriterCache(lruTaxonomyWriterCacheSize))

    def numerateTerm(self, term):
        if not term:
            return
        return self._taxoWriter.addCategory(FacetLabel([term]))

    def getTerm(self, nr):
        if not hasattr(self, "_taxoReader"):
            self._taxoReader = DirectoryTaxonomyReader(self._taxoWriter)
        tr = DirectoryTaxonomyReader.openIfChanged(self._taxoReader)
        if tr:
            self._taxoReader.close()
            self._taxoReader = tr
        return self._taxoReader.getPath(nr).components[0]

    def commit(self):
        self._taxoWriter.commit()

    def handleShutdown(self):
        print 'handle shutdown: saving TermNumerator'
        from sys import stdout; stdout.flush()
        self.commit()

    def close(self):
        self._taxoWriter.close()

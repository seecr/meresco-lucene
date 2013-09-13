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

from org.apache.lucene.store import SimpleFSDirectory
from org.apache.lucene.facet.taxonomy.directory import DirectoryTaxonomyWriter
from org.apache.lucene.facet.taxonomy import CategoryPath
from java.io import File

from meresco.core import Observable

class TermNumerator(Observable):

    def __init__(self, path):
        Observable.__init__(self)
        self._taxoDirectory = SimpleFSDirectory(File(path))
        self._taxoWriter = DirectoryTaxonomyWriter(self._taxoDirectory)

    def numerateTerm(self, term):
        return self._taxoWriter.addCategory(CategoryPath([term]))

    def handleShutdown(self):
        self._taxoWriter.commit()

    def close(self):
        self._taxoWriter.close()
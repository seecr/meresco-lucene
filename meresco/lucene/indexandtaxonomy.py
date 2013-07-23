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

from org.apache.lucene.index import DirectoryReader
from org.apache.lucene.search import IndexSearcher
from org.apache.lucene.facet.taxonomy.directory import DirectoryTaxonomyReader


class IndexAndTaxonomy(object):

    def __init__(self, indexWriter=None, taxoDirectory=None, copied=None):
        if copied is not None:
            self.searcher = copied['searcher']
            self.taxoReader = copied['taxoReader']
        else:
            self.searcher = IndexSearcher(DirectoryReader.open(indexWriter, True))
            self.taxoReader = DirectoryTaxonomyReader(taxoDirectory)

    def tryIncRef(self):
        indexReader = self.searcher.getIndexReader()
        if indexReader.tryIncRef():
            if self.taxoReader.tryIncRef():
                return True
            else:
                indexReader.decRef()
        return False

    def decRef(self):
        self.taxoReader.decRef()
        self.searcher.getIndexReader().decRef()

    def refreshIfNeeded(currentIndexAndTaxonomy):
        currentReader = currentIndexAndTaxonomy.searcher.getIndexReader()
        reader = DirectoryReader.openIfChanged(currentReader)
        if reader is None:
            return currentIndexAndTaxonomy
        searcher = IndexSearcher(reader)
        taxoReader = DirectoryTaxonomyReader.openIfChanged(currentIndexAndTaxonomy.taxoReader)
        if taxoReader is None:
            taxoReader = currentIndexAndTaxonomy.taxoReader
            taxoReader.incRef()
        result = IndexAndTaxonomy(copied=dict(searcher=searcher, taxoReader=taxoReader))
        return result

    def __del__(self):
        self.decRef()
        self.searcher = None
        self.taxoReader = None

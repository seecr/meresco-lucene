## begin license ##
#
# "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
#
# Copyright (C) 2014 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
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

from seecr.test import SeecrTestCase
from java.util.concurrent import Executors
from org.apache.lucene.store import SimpleFSDirectory
from java.io import File
from org.apache.lucene.index import IndexWriterConfig, IndexWriter, DirectoryReader, AtomicReaderContext
from org.apache.lucene.util import Version
from org.meresco.lucene.search import SuperIndexSearcher
from org.meresco.lucene.test import DummyIndexReader
from java.util import ArrayList
from org.meresco.lucene.analysis import MerescoStandardAnalyzer

class SuperIndexSearcherTest(SeecrTestCase):

    def setUp(self):
        super(SuperIndexSearcherTest, self).setUp()
        self.executor = Executors.newFixedThreadPool(5)
        indexDirectory = SimpleFSDirectory(File(self.tempdir))
        conf = IndexWriterConfig(Version.LUCENE_4_10_0, MerescoStandardAnalyzer())
        self.writer = IndexWriter(indexDirectory, conf)
        self.reader = DirectoryReader.open(self.writer, True)
        self.sis = SuperIndexSearcher(self.reader)

    def testGroupLeaves(self):
        contexts = ArrayList().of_(AtomicReaderContext)
        contexts.add(DummyIndexReader.dummyIndexReader(10).getContext())
        result = self.sis.group_leaves_test(contexts, 5)
        self.assertEquals(1, result.size())
        firstContext = ArrayList().of_(AtomicReaderContext).cast_(result.get(0))
        self.assertEquals(1, firstContext.size())

    def testGroupLeaves1ForEach(self):
        contexts = ArrayList().of_(AtomicReaderContext)
        contexts.add(DummyIndexReader.dummyIndexReader(10).getContext())
        contexts.add(DummyIndexReader.dummyIndexReader(9).getContext())
        contexts.add(DummyIndexReader.dummyIndexReader(8).getContext())
        contexts.add(DummyIndexReader.dummyIndexReader(7).getContext())
        contexts.add(DummyIndexReader.dummyIndexReader(6).getContext())
        result = self.sis.group_leaves_test(contexts, 5)
        self.assertEquals(5, result.size())
        for i in range(5):
            context = ArrayList().of_(AtomicReaderContext).cast_(result.get(i))
            self.assertEquals(1, context.size())

    def testGroupLeaves1TooMuch(self):
        contexts = ArrayList().of_(AtomicReaderContext)
        contexts.add(DummyIndexReader.dummyIndexReader(10).getContext())
        contexts.add(DummyIndexReader.dummyIndexReader(9).getContext())
        contexts.add(DummyIndexReader.dummyIndexReader(8).getContext())
        contexts.add(DummyIndexReader.dummyIndexReader(7).getContext())
        contexts.add(DummyIndexReader.dummyIndexReader(6).getContext())
        contexts.add(DummyIndexReader.dummyIndexReader(5).getContext())
        result = self.sis.group_leaves_test(contexts, 5)
        self.assertEquals(5, result.size())
        for i in range(4):
            context = ArrayList().of_(AtomicReaderContext).cast_(result.get(i))
            self.assertEquals(1, context.size())
        context = ArrayList().of_(AtomicReaderContext).cast_(result.get(4))
        self.assertEquals(2, context.size())

    def testGroupLeavesAllDouble(self):
        contexts = ArrayList().of_(AtomicReaderContext)
        contexts.add(DummyIndexReader.dummyIndexReader(10).getContext())
        contexts.add(DummyIndexReader.dummyIndexReader(9).getContext())
        contexts.add(DummyIndexReader.dummyIndexReader(8).getContext())
        contexts.add(DummyIndexReader.dummyIndexReader(7).getContext())
        contexts.add(DummyIndexReader.dummyIndexReader(6).getContext())
        contexts.add(DummyIndexReader.dummyIndexReader(5).getContext())
        contexts.add(DummyIndexReader.dummyIndexReader(4).getContext())
        contexts.add(DummyIndexReader.dummyIndexReader(3).getContext())
        contexts.add(DummyIndexReader.dummyIndexReader(2).getContext())
        contexts.add(DummyIndexReader.dummyIndexReader(1).getContext())
        result = self.sis.group_leaves_test(contexts, 5)
        self.assertEquals(5, result.size())
        for i in range(5):
            context = ArrayList().of_(AtomicReaderContext).cast_(result.get(i))
            self.assertEquals(2, context.size())
            totalDocs = AtomicReaderContext.cast_(context.get(0)).reader().numDocs() + AtomicReaderContext.cast_(context.get(1)).reader().numDocs()
            self.assertEquals(11, totalDocs)

    def testFindSmallestSlice(self):
        self.assertEquals(0, self.sis.find_smallest_slice_test([0, 0, 0, 0, 0]))
        self.assertEquals(1, self.sis.find_smallest_slice_test([1, 0, 0, 0, 0]))
        self.assertEquals(1, self.sis.find_smallest_slice_test([1, 0, 1, 0, 0]))
        self.assertEquals(0, self.sis.find_smallest_slice_test([1, 1, 1, 1, 1]))
        self.assertEquals(4, self.sis.find_smallest_slice_test([2, 1, 1, 1, 0]))
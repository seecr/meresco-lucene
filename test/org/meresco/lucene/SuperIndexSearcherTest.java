/* begin license *
 *
 * "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
 *
 * Copyright (C) 2015 Koninklijke Bibliotheek (KB) http://www.kb.nl
 * Copyright (C) 2015 Seecr (Seek You Too B.V.) http://seecr.nl
 *
 * This file is part of "Meresco Lucene"
 *
 * "Meresco Lucene" is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * "Meresco Lucene" is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with "Meresco Lucene"; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
 *
 * end license */

package org.meresco.lucene;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.SimpleFSDirectory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.meresco.lucene.analysis.MerescoStandardAnalyzer;
import org.meresco.lucene.search.SuperIndexSearcher;
import org.meresco.lucene.test.DummyIndexReader;

public class SuperIndexSearcherTest extends SeecrTestCase {

    private IndexWriter writer;
    private DirectoryReader reader;
    private SuperIndexSearcher sis;
    private ExecutorService executor;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        this.executor = Executors.newFixedThreadPool(5);
        Directory indexDirectory = new SimpleFSDirectory(this.tmpDir);
        IndexWriterConfig conf = new IndexWriterConfig(new MerescoStandardAnalyzer());
        this.writer = new IndexWriter(indexDirectory, conf);
        this.reader = DirectoryReader.open(this.writer, true, true);
        this.sis = new SuperIndexSearcher(this.reader);
    }

    @After
    public void tearDown() throws Exception {
        this.reader.close();
        this.writer.close();
        this.executor.shutdownNow();
        super.tearDown();
    }

    @Test
    public void testGroupLeaves() {
        List<LeafReaderContext> contexts = new ArrayList<LeafReaderContext>();
        contexts.add(DummyIndexReader.dummyIndexReader(10).getContext());
        List<List<LeafReaderContext>> result = this.sis.group_leaves_test(contexts, 5);
        assertEquals(1, result.size());
        List<LeafReaderContext> firstContext = result.get(0);
        assertEquals(1, firstContext.size());
    }

    @Test
    public void testGroupLeaves1ForEach() {
        ArrayList<LeafReaderContext> contexts = new ArrayList<LeafReaderContext>();
        contexts.add(DummyIndexReader.dummyIndexReader(10).getContext());
        contexts.add(DummyIndexReader.dummyIndexReader(9).getContext());
        contexts.add(DummyIndexReader.dummyIndexReader(8).getContext());
        contexts.add(DummyIndexReader.dummyIndexReader(7).getContext());
        contexts.add(DummyIndexReader.dummyIndexReader(6).getContext());
        List<List<LeafReaderContext>> result = this.sis.group_leaves_test(contexts, 5);
        assertEquals(5, result.size());
        for (int i = 0; i < 5; i++) {
            List<LeafReaderContext> context = result.get(i);
            assertEquals(1, context.size());
        }
    }

    @Test
    public void testGroupLeaves1TooMuch() {
        ArrayList<LeafReaderContext> contexts = new ArrayList<LeafReaderContext>();
        contexts.add(DummyIndexReader.dummyIndexReader(10).getContext());
        contexts.add(DummyIndexReader.dummyIndexReader(9).getContext());
        contexts.add(DummyIndexReader.dummyIndexReader(8).getContext());
        contexts.add(DummyIndexReader.dummyIndexReader(7).getContext());
        contexts.add(DummyIndexReader.dummyIndexReader(6).getContext());
        contexts.add(DummyIndexReader.dummyIndexReader(5).getContext());
        List<List<LeafReaderContext>> result = this.sis.group_leaves_test(contexts, 5);
        assertEquals(5, result.size());
        for (int i = 0; i < 4; i++) {
            List<LeafReaderContext> context = result.get(i);
            assertEquals(1, context.size());
        }
        List<LeafReaderContext> context = result.get(4);
        assertEquals(2, context.size());
    }

    @Test
    public void testGroupLeavesAllDouble() {
        ArrayList<LeafReaderContext> contexts = new ArrayList<LeafReaderContext>();
        contexts.add(DummyIndexReader.dummyIndexReader(10).getContext());
        contexts.add(DummyIndexReader.dummyIndexReader(9).getContext());
        contexts.add(DummyIndexReader.dummyIndexReader(8).getContext());
        contexts.add(DummyIndexReader.dummyIndexReader(7).getContext());
        contexts.add(DummyIndexReader.dummyIndexReader(6).getContext());
        contexts.add(DummyIndexReader.dummyIndexReader(5).getContext());
        contexts.add(DummyIndexReader.dummyIndexReader(4).getContext());
        contexts.add(DummyIndexReader.dummyIndexReader(3).getContext());
        contexts.add(DummyIndexReader.dummyIndexReader(2).getContext());
        contexts.add(DummyIndexReader.dummyIndexReader(1).getContext());
        List<List<LeafReaderContext>> result = this.sis.group_leaves_test(contexts, 5);
        assertEquals(5, result.size());
        for (int i = 0; i < 4; i++) {
            List<LeafReaderContext> context = result.get(i);
            assertEquals(2, context.size());
            int totalDocs = context.get(0).reader().numDocs() + context.get(1).reader().numDocs();
            assertEquals(11, totalDocs);
        }

    }

    @Test
    public void testFindSmallestSlice() {
        assertEquals(0, this.sis.find_smallest_slice_test(new int[] {0, 0, 0, 0, 0}));
        assertEquals(1, this.sis.find_smallest_slice_test(new int[] {1, 0, 0, 0, 0}));
        assertEquals(1, this.sis.find_smallest_slice_test(new int[] {1, 0, 1, 0, 0}));
        assertEquals(0, this.sis.find_smallest_slice_test(new int[] {1, 1, 1, 1, 1}));
        assertEquals(4, this.sis.find_smallest_slice_test(new int[] {2, 1, 1, 1, 0}));
    }
}

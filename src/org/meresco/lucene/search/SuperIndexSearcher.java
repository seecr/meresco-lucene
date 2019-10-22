/* begin license *
 *
 * "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
 *
 * Copyright (C) 2014, 2016 Seecr (Seek You Too B.V.) http://seecr.nl
 * Copyright (C) 2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
 * Copyright (C) 2016 Koninklijke Bibliotheek (KB) http://www.kb.nl
 * Copyright (C) 2016 Stichting Kennisnet http://www.kennisnet.nl
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

package org.meresco.lucene.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Weight;

public class SuperIndexSearcher extends IndexSearcher {

    private ExecutorService executor;
    private List<List<LeafReaderContext>> grouped_leaves;

    public SuperIndexSearcher(IndexReader reader, ExecutorService executor, int tasks) {
        super(reader);
        this.executor = executor;
        this.grouped_leaves = this.group_leaves(reader.leaves(), tasks);
//        for (List<LeafReaderContext> l : this.grouped_leaves) {
//            int t = 0;
//            for (LeafReaderContext ctx : l)
//                t += ctx.reader().numDocs();
//             System.out.print(" " + t + " ");
//        }
//        System.out.println();
    }

    private List<List<LeafReaderContext>> group_leaves(List<LeafReaderContext> leaves, int tasks) {
        List<List<LeafReaderContext>> slices = new ArrayList<List<LeafReaderContext>>(tasks);
        for (int i = 0; i < tasks; i++)
            slices.add(new ArrayList<LeafReaderContext>());
        int sizes[] = new int[tasks];
        int max_i = 0;
        for (LeafReaderContext context : leaves) {
            int smallest_i = find_smallest_slice(sizes);
            slices.get(smallest_i).add(context);
            sizes[smallest_i] += context.reader().numDocs();
            if (smallest_i > max_i)
                max_i = smallest_i;
        }
        return slices.subList(0, max_i + 1);
    }

    private int find_smallest_slice(int[] sizes) {
        int smallest = Integer.MAX_VALUE;
        int smallest_i = 0;
        for (int i = 0; i < sizes.length; i++)
            if (sizes[i] < smallest) {
                smallest = sizes[i];
                smallest_i = i;
            }
        return smallest_i;
    }

    public void search(Query q, SuperCollector<?> c) throws Throwable {
        SubCollector subCollector = c.subCollector();
        float boost = 1.0f;

        // NEEDS WORK: was createNormalizedWeight call
        // See: 
        // https://lucene.apache.org/core/7_6_0/core/org/apache/lucene/search/IndexSearcher.html#createNormalizedWeight-org.apache.lucene.search.Query-boolean-
        Weight weight = super.createWeight(q, subCollector.scoreMode(), boost);
        ExecutorCompletionService<String> ecs = new ExecutorCompletionService<String>(this.executor);
        List<Future<String>> futures = new ArrayList<Future<String>>();
        for (List<LeafReaderContext> leaf_group : this.grouped_leaves.subList(1, this.grouped_leaves.size()))
            futures.add(ecs.submit(new SearchTask(leaf_group, weight, c.subCollector()), "Done"));
        new SearchTask(this.grouped_leaves.get(0), weight, subCollector).run();
        try {
            for (int i = 0; i < this.grouped_leaves.size() - 1; i++) {
                ecs.take().get();
            }
        } catch (ExecutionException e) {
            throw e.getCause();
        } finally {
            for (Future<String> future : futures)
                future.cancel(true);
        }
        c.complete();
    }

    public class SearchTask implements Runnable {
        private List<LeafReaderContext> contexts;
        private Weight weight;
        private SubCollector subCollector;

        public SearchTask(List<LeafReaderContext> contexts, Weight weight, SubCollector subCollector) {
            this.contexts = contexts;
            this.weight = weight;
            this.subCollector = subCollector;
        }

        @Override
        public void run() {
            try {
                SuperIndexSearcher.this.search(this.contexts, this.weight, this.subCollector);
                this.subCollector.complete();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public SuperIndexSearcher(DirectoryReader reader) {
        super(reader);
    }

    public int find_smallest_slice_test(int[] sizes) {
        return find_smallest_slice(sizes);
    }

    public List<List<LeafReaderContext>> group_leaves_test(List<LeafReaderContext> leaves, int tasks) {
        return group_leaves(leaves, tasks);
    }
}

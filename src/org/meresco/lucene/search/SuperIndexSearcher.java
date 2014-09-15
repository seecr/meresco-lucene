/* begin license *
 *
 * "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
 *
 * Copyright (C) 2014 Seecr (Seek You Too B.V.) http://seecr.nl
 * Copyright (C) 2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
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
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Weight;

public class SuperIndexSearcher extends IndexSearcher {

    private ExecutorService executor;
    private List<List<AtomicReaderContext>> grouped_leaves;

    public SuperIndexSearcher(DirectoryReader reader, ExecutorService executor, int tasks) {
        super(reader);
        this.executor = executor;
        this.grouped_leaves = this.group_leaves(reader.leaves(), tasks);
        for (List<AtomicReaderContext> l : this.grouped_leaves) {
            int t = 0;
            for (AtomicReaderContext ctx : l)
                t += ctx.reader().numDocs();
            System.out.print(" " + t + " ");
        }
        System.out.println();
    }

    protected List<List<AtomicReaderContext>> group_leaves(List<AtomicReaderContext> leaves, int tasks) {
        List<List<AtomicReaderContext>> slices = new ArrayList<List<AtomicReaderContext>>(tasks);
        for (int i = 0; i < tasks; i++)
            slices.add(new ArrayList<AtomicReaderContext>());
        int sizes[] = new int[tasks];
        for (AtomicReaderContext context : leaves) {
            int smallest_i = find_smallest_slice(sizes);
            slices.get(smallest_i).add(context);
            sizes[smallest_i] += context.reader().numDocs();
        }
        return slices;
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

    public void search(Query q, Filter f, SuperCollector<?> c) throws IOException, InterruptedException,
            ExecutionException {
        Weight weight = super.createNormalizedWeight(wrapFilter(q, f));
        ExecutorCompletionService<String> ecs = new ExecutorCompletionService<String>(this.executor);
        for (List<AtomicReaderContext> leaf_group : this.grouped_leaves)
            ecs.submit(new SearchTask(leaf_group, weight, c.subCollector()), "Done");
        for (int i = 0; i < this.grouped_leaves.size(); i++) {
            ecs.take().get();
        }
    }

    public class SearchTask implements Runnable {
        private List<AtomicReaderContext> contexts;
        private Weight weight;
        private SubCollector subCollector;

        public SearchTask(AtomicReaderContext context, Weight weight, SubCollector subCollector) {
            this(Arrays.asList(context), weight, subCollector);
        }

        public SearchTask(List<AtomicReaderContext> contexts, Weight weight, SubCollector subCollector) {
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
}

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
import java.util.Collections;
import java.util.Comparator;
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
    private ArrayList<AtomicReaderContext> bigSegments;
    private ArrayList<AtomicReaderContext> smallSegments;

    public SuperIndexSearcher(DirectoryReader reader, ExecutorService executor) {
        super(reader);
        this.executor = executor;
        findBigSegments();
    }

    private void findBigSegments() {
        this.bigSegments = new ArrayList<AtomicReaderContext>(super.leafContexts);
        Collections.sort(bigSegments, new Comparator<AtomicReaderContext>() {
            public int compare(AtomicReaderContext lhs, AtomicReaderContext rhs) {
                return Integer.compare(lhs.reader().maxDoc(), rhs.reader().maxDoc());
            }
        });
        int totalSmallDocs = 0;
        this.smallSegments = new ArrayList<AtomicReaderContext>();
        if (this.bigSegments.isEmpty())
            return;
        int largestSegment = this.bigSegments.get(this.bigSegments.size() - 1).reader().maxDoc();
        while (totalSmallDocs + this.bigSegments.get(0).reader().maxDoc() <= largestSegment) {
            AtomicReaderContext context = this.bigSegments.remove(0);
            totalSmallDocs += context.reader().maxDoc();
            this.smallSegments.add(context);
            if (this.bigSegments.isEmpty())
                return;
        }
    }

    public void search(Query q, Filter f, SuperCollector<?> c) throws IOException, InterruptedException,
            ExecutionException {
        Weight weight = super.createNormalizedWeight(wrapFilter(q, f));
        ExecutorCompletionService<String> ecs = new ExecutorCompletionService<String>(this.executor);
        for (AtomicReaderContext ctx : this.bigSegments)
            ecs.submit(new SearchTask(ctx, weight, c.subCollector()), "Done");
        new SearchTask(this.smallSegments, weight, c.subCollector()).run();
        for (int i = 0; i < this.bigSegments.size(); i++) {
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

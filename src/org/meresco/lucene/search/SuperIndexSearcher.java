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

	public SuperIndexSearcher(DirectoryReader reader, ExecutorService executor) {
		super(reader);
		this.executor = executor;
	}

	public void search(Query q, Filter f, SuperCollector<?> c) throws IOException, InterruptedException,
			ExecutionException {
		Weight weight = super.createNormalizedWeight(wrapFilter(q, f));
		ExecutorCompletionService<String> ecs = new ExecutorCompletionService<String>(this.executor);
		// IDEA 1: group small leaves and sumbit those groups to avoid overhead
		// IDEA 2: submit large leaves to pool, do small leaves her in main
		// thread
		System.out.println(this.getIndexReader().maxDoc());
		// ecs.submit(new SearchTask(super.leafContexts, weight, c.subCollector(ctx)), "Done");
		for (AtomicReaderContext ctx : super.leafContexts) {
			System.out.println("Ctx: " + ctx.reader().maxDoc());
			ecs.submit(new SearchTask(weight, c.subCollector(), ctx), "Done");
		}
		for (int i = 0; i < super.leafContexts.size(); i++) {
			ecs.take().get();
		}
	}

	public class SearchTask implements Runnable {
		private List<AtomicReaderContext> contexts;
		private Weight weight;
		private SubCollector subCollector;

		public SearchTask(Weight weight, SubCollector subCollector, AtomicReaderContext... leaves) {
			this.contexts = Arrays.asList(leaves);
			this.weight = weight;
			this.subCollector = subCollector;
		}

		@Override
		public void run() {
			long t0 = System.currentTimeMillis();
			try {
				SuperIndexSearcher.this.search(this.contexts, this.weight, this.subCollector);
				this.subCollector.complete();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
			long t1 = System.currentTimeMillis();
			System.out.println("Task took " + (t1 - t0) + " for docBase" + this.contexts.get(0).docBase);
		}
	}
}

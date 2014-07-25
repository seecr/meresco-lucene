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

public class ThreadedIndexSearcher extends IndexSearcher {

	private ExecutorService executor;

	public ThreadedIndexSearcher(DirectoryReader reader, ExecutorService executor) {
		super(reader);
		this.executor = executor;
	}

	public void search(Query Q, Filter F, SuperCollector C) throws IOException, InterruptedException,
			ExecutionException {
		Weight weight = super.createNormalizedWeight(wrapFilter(Q, F));
		ExecutorCompletionService<String> ecs = new ExecutorCompletionService<String>(this.executor);
		// IDEA 1: group small leaves and sumbit those groups to avoid overhead
		// IDEA 2: submit large leaves to pool, do small leaves her in main thread
		for (AtomicReaderContext ctx : super.leafContexts) {
			ecs.submit(new SearchTask(ctx, weight, C.subCollector(ctx)), "Done");
		}
		for (AtomicReaderContext _ : super.leafContexts) {
			ecs.take().get();
		}
	}

	public class SearchTask implements Runnable {
		private List<AtomicReaderContext> contexts;
		private Weight weight;
		private SubCollector subCollector;

		public SearchTask(AtomicReaderContext context, Weight weight, SubCollector subCollector) {
			this.contexts = Arrays.asList(context);
			this.weight = weight;
			this.subCollector = subCollector;
		}

		@Override
		public void run() {
			try {
				ThreadedIndexSearcher.this.search(this.contexts, this.weight, this.subCollector);
				this.subCollector.complete();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}
}

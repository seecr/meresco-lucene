package org.meresco.lucene.search;

import java.io.IOException;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Collector;

public abstract class SubCollector extends Collector {

	/**
	 * This method signals completion of the collect phase. It gives the
	 * SubCollector a chance to do additional work in the same (separate)
	 * thread, like post-processing results, for example, accumulating facets
	 * for this segment.
	 * 
	 * It is up to the SuperCollector to gather results from SubCollectors.
	 * 
	 * @throws IOException
	 */
	public abstract void complete() throws IOException;
}
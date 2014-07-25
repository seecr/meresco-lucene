package org.meresco.lucene.search;

import java.io.IOException;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Scorer;

public abstract class SubCollector extends Collector {

	private final AtomicReaderContext context;

	/**
	 * Created for collecting from one {@link AtomicReaderContext} in a single
	 * thread.
	 * 
	 * @param context
	 *            atomic reader context this SubCollector is bound to.
	 */
	public SubCollector(AtomicReaderContext context) {
		this.context = context;
	}

	/**
	 * This method exists to provide backwards compatibility to the
	 * {@link Collector} interface. It only assures that it is used correctly.
	 * It can be dropped in the future.
	 */
	@Override
	final public void setNextReader(AtomicReaderContext context) throws IOException {
		assert this.context == context;
	}

	  /**
	   * This method signals completion of the collect phase.  It gives the SubCollector a
	   * chance to do additional work in the same (separate) thread, like post-processing
	   * results, for example, accumulating facets for this segment.
	   * 
	   * It is up to the SuperCollector to gather results from SubCollectors.
	   */
	public abstract void complete();
}
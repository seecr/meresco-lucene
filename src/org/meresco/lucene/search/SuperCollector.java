package org.meresco.lucene.search;

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.index.AtomicReaderContext;

public abstract class SuperCollector<SubCollectorType extends SubCollector> {

	private List<SubCollectorType> subs = new ArrayList<SubCollectorType>();

	/**
	 * Called before collecting from each {@link AtomicReaderContext} in a
	 * separate thread. The returned {@link SubCollector} need not be thread
	 * safe as it's scope is limited to one segment.
	 * 
	 * The SubCollector is kept in a list and accessible by
	 * {@link #subCollectors()}.
	 * 
	 * @param context
	 *            next atomic reader context
	 */
	public SubCollector subCollector(AtomicReaderContext context) {
		SubCollectorType sub = this.createSubCollector(context);
		this.subs.add(sub);
		return sub;
	}

	/**
	 * Lower level factory method for SubCollectors.
	 * 
	 * @param context
	 *            is an AtomicReaderContext
	 * @return SubCollector for this context
	 */
	abstract protected SubCollectorType createSubCollector(AtomicReaderContext context);

	/**
	 * Gives access to all SubCollectors of this SuperCollector.
	 * 
	 * @return an Iterable containing SubCollectors
	 */
	protected Iterable<SubCollectorType> subCollectors() {
		return subs;
	}
}

package org.meresco.lucene.search.join;

import java.io.IOException;

import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Query;

public interface HasKeySetCache {
	public void cleanupPreviousCollect();
	
    public DocIdSet getCollectedKeys() throws IOException;
    
    public Query getQuery();

	public Object getCacheKey();
	
	public void printKeySetCacheSize();
}

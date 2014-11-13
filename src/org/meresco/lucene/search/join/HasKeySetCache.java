package org.meresco.lucene.search.join;

import java.io.IOException;

import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Query;

public interface HasKeySetCache {
    public DocIdSet getCollectedKeys() throws IOException;
    
    public Query getQuery();

	public void printKeySetCacheSize();
}

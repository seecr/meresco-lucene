package org.meresco.lucene.search.join;

import java.io.IOException;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BitSet;
import org.meresco.lucene.Lucene;
import org.meresco.lucene.queries.KeyFilter;

public class LuceneQuery implements RelationalQuery {
    private Lucene lucene;
    private Query q;
    private String rank;
    private KeySuperCollector keyCollector = null;

    public LuceneQuery(Lucene lucene,Query q) {
        this(lucene, q, null);
    }

    public LuceneQuery(Lucene lucene, Query q, String rank) {
        this.lucene = lucene;
        this.q = q;
        this.rank = rank;
    }

    @Override
	public Result execute() {
        
        try {
            this.lucene.search(this.q, keyCollector);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
        System.out.println("collected keys: " + keyCollector.getCollectedKeys());
        return new Result(keyCollector.getCollectedKeys());
    }
    
    @Override
    public void prepareCollectKeys(String keyName) {
        this.keyCollector  = new KeySuperCollector(keyName);
    }

    @Override
	public void addFilter(BitSet bitset, String keyName) {
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        builder.add(q, BooleanClause.Occur.MUST);
        try {
            builder.add(new KeyFilter(bitset, keyName), BooleanClause.Occur.MUST);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        this.q = builder.build();
    }

}
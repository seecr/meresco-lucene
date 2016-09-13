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
    private String keyName;
    private Query q;
    private String rank;

    public LuceneQuery(Lucene lucene, String keyName, Query q) {
        this(lucene, keyName, q, null);
    }

    public LuceneQuery(Lucene lucene, String keyName, Query q, String rank) {
        this.lucene = lucene;
        this.keyName = keyName;
        this.q = q;
        this.rank = rank;
    }

    @Override
	public Result execute() {
        KeySuperCollector keyCollector = new KeySuperCollector(this.keyName);
        try {
            this.lucene.search(this.q, keyCollector);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
        System.out.println("collected keys: " + keyCollector.getCollectedKeys());
        return new Result(keyCollector.getCollectedKeys());
    }

    @Override
	public void addFilter(BitSet bitset) {
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        builder.add(q, BooleanClause.Occur.MUST);
        try {
            builder.add(new KeyFilter(bitset, this.keyName), BooleanClause.Occur.MUST);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        this.q = builder.build();
    }

}
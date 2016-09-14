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
    private String collectKeyName;
    private String filterKeyName;
    private Query q;
    private String rank;

    public LuceneQuery(Lucene lucene, String keyName, Query q) {
        this(lucene, keyName, keyName, q);
    }

    public LuceneQuery(Lucene lucene, String collectKeyName, String filterKeyName, Query q) {
		this(lucene, collectKeyName, filterKeyName, q, null);
	}

    public LuceneQuery(Lucene lucene, String collectKeyName, String filterKeyName, Query q, String rank) {
        this.lucene = lucene;
        this.collectKeyName = collectKeyName;
        this.filterKeyName = filterKeyName;
        this.q = q;
        this.rank = rank;
    }



	@Override
	public Result execute() {
        KeySuperCollector keyCollector = new KeySuperCollector(this.collectKeyName);
        try {
            this.lucene.search(this.q, keyCollector);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
        return new Result(keyCollector.getCollectedKeys());
    }

    @Override
	public void addFilter(BitSet bitset) {
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        builder.add(q, BooleanClause.Occur.MUST);
        try {
            builder.add(new KeyFilter(bitset, this.filterKeyName), BooleanClause.Occur.MUST);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        this.q = builder.build();
    }
}
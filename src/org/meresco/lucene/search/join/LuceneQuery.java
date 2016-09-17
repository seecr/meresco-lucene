package org.meresco.lucene.search.join;

import java.io.IOException;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.meresco.lucene.Lucene;
import org.meresco.lucene.queries.KeyFilter;


public class LuceneQuery implements RelationalQuery {
    private Lucene lucene;
    private String collectKeyName;
    private String filterKeyName;
    private Query originalQ;
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
        this.originalQ = q;
        this.q = q;
        this.rank = rank;
    }

    @Override
	public String toString() {
    	return "LuceneQuery(" + this.lucene + ", " + this.collectKeyName + ", " + this.filterKeyName + ", " + this.originalQ + ")";
    }

	@Override
	public IntermediateResult execute() {
		System.out.println("execute " + this);
        KeySuperCollector keyCollector = new KeySuperCollector(this.collectKeyName);
        try {
        	System.out.println("search " + this.q);
            this.lucene.search(this.q, keyCollector);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
        IntermediateResult result = new IntermediateResult(keyCollector.getCollectedKeys());
        System.out.println("result: " + result);
        return result;
    }

	@Override
	public void invert() {
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        builder.add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST);
        builder.add(this.q, BooleanClause.Occur.MUST_NOT);
        this.q = builder.build();
	}

    @Override
	public void filter(IntermediateResult keyFilter) {
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        builder.add(this.q, BooleanClause.Occur.MUST);
        try {
        	BooleanClause.Occur occur = keyFilter.inverted ? BooleanClause.Occur.MUST_NOT : BooleanClause.Occur.MUST;
            builder.add(new KeyFilter(keyFilter.getBitSet(), this.filterKeyName), occur);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        this.q = builder.build();
    }

	@Override
	public void union(IntermediateResult intermediateResult) {
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        try {
        	BooleanClause.Occur occur = intermediateResult.inverted ? BooleanClause.Occur.MUST_NOT : BooleanClause.Occur.SHOULD;
            builder.add(new KeyFilter(intermediateResult.getBitSet(), this.filterKeyName), occur);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        builder.add(this.q, BooleanClause.Occur.SHOULD);
        this.q = builder.build();
	}
}
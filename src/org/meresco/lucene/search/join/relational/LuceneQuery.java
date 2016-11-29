package org.meresco.lucene.search.join.relational;

import java.io.IOException;
import java.util.Map;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.meresco.lucene.Lucene;
import org.meresco.lucene.queries.KeyFilter;
import org.meresco.lucene.search.join.KeySuperCollector;


public class LuceneQuery implements RelationalQuery {
    private String core;
    private String collectKeyName;
    private String filterKeyName;
    private Query originalQ;
    private Query q;
    private String boost;

    public LuceneQuery(String core, String keyName, Query q) {
        this(core, keyName, keyName, q);
    }

    public LuceneQuery(String core, String collectKeyName, String filterKeyName, Query q) {
		this(core, collectKeyName, filterKeyName, q, null);
	}

    public LuceneQuery(String core, String collectKeyName, String filterKeyName, Query q, String boost) {
        this.core = core;
        this.collectKeyName = collectKeyName;
        this.filterKeyName = filterKeyName;
        this.originalQ = q;
        this.q = q;
        this.boost = boost;
    }

    @Override
	public String toString() {
    	return "LuceneQuery(" + this.core + ", " + this.collectKeyName + ", " + this.filterKeyName + ", " + this.originalQ + ")";
    }

	@Override
	public IntermediateResult execute(Map<String, Lucene> lucenes) {
//		System.out.println("execute " + this);
        KeySuperCollector keyCollector = new KeySuperCollector(this.collectKeyName);
        try {
//        	System.out.println("search " + this.q);
            lucenes.get(this.core).search(this.q, keyCollector);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
        IntermediateResult result = new IntermediateResult(keyCollector.getCollectedKeys());
//        System.out.println("result: " + result);
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
	public void filter(IntermediateResult intermediateResult) {
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        builder.add(this.q, BooleanClause.Occur.MUST);
        try {
            builder.add(new KeyFilter(intermediateResult.getBitSet(), this.filterKeyName, intermediateResult.inverted), BooleanClause.Occur.MUST);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        this.q = builder.build();
    }

	@Override
	public void union(IntermediateResult intermediateResult) {
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        try {
        	builder.add(new KeyFilter(intermediateResult.getBitSet(), this.filterKeyName, intermediateResult.inverted), BooleanClause.Occur.SHOULD);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        builder.add(this.q, BooleanClause.Occur.SHOULD);
        this.q = builder.build();
	}
}
/* begin license *
 *
 * "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
 *
 * Copyright (C) 2016 Seecr (Seek You Too B.V.) http://seecr.nl
 *
 * This file is part of "Meresco Lucene"
 *
 * "Meresco Lucene" is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * "Meresco Lucene" is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with "Meresco Lucene"; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
 *
 * end license */

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
    private float boost;

    public LuceneQuery(String core, String keyName, Query q) {
        this(core, keyName, keyName, q);
    }

    public LuceneQuery(String core, String collectKeyName, String filterKeyName, Query q) {
		this(core, collectKeyName, filterKeyName, q, 1.0f);
	}

    public LuceneQuery(String core, String collectKeyName, String filterKeyName, Query q, float boost) {
        this.core = core;
        this.collectKeyName = collectKeyName;
        this.filterKeyName = filterKeyName;
        this.originalQ = q;
        this.q = q;
        this.boost = boost;  // ignored so far
    }

    @Override
	public String toString() {
    	return "LuceneQuery(\"" + this.core + "\", \"" + this.collectKeyName + "\", \"" + this.filterKeyName + "\", " + this.originalQ + ")";
    }

	@Override
	public IntermediateResult collectKeys(Map<String, Lucene> lucenes) {
//        System.out.println("execute " + this);
        KeySuperCollector keyCollector = new KeySuperCollector(this.collectKeyName);
        try {
//            System.out.println("search " + this.q);
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
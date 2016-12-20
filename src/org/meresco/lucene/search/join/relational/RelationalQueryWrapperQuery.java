package org.meresco.lucene.search.join.relational;

import org.apache.lucene.search.Query;


public class RelationalQueryWrapperQuery extends Query {
	public RelationalQuery relationalQuery;

	public RelationalQueryWrapperQuery(RelationalQuery rq) {
		this.relationalQuery = rq;
	}


	@Override
	public String toString(String field) {
		return "RelationalQueryWrapperQuery(" + this.relationalQuery + ")";
	}

	@Override
	public boolean equals(Object obj) {
		return false;
	}

	@Override
	public int hashCode() {
		return 0;
	}
}

package org.meresco.lucene.search.join.relational;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Weight;


public class WrappedRelationalQuery extends Query {
    public RelationalQuery relationalQuery;


    public WrappedRelationalQuery(RelationalQuery rq) {
        this.relationalQuery = rq;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, boolean needsScores) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString(String field) {
        return getClass().getSimpleName() + "(" + this.relationalQuery + ")";
    }

    @Override
    public boolean equals(Object obj) {
        return (obj.getClass() == this.getClass() && ((WrappedRelationalQuery) obj).relationalQuery == this.relationalQuery);
    }

    @Override
    public int hashCode() {
        return 1 + 37 * this.relationalQuery.hashCode();
    }
}

package org.meresco.lucene.search.join.relational;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Weight;


public class WrappedRelationalQuery extends Query {
    public RelationalQuery relationalQuery;


    public WrappedRelationalQuery(RelationalQuery rq) {
        assert rq != null;
        this.relationalQuery = rq;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, boolean needsScores, float boost) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString(String field) {
        return getClass().getSimpleName() + "(" + this.relationalQuery + ")";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((relationalQuery == null) ? 0 : relationalQuery.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        WrappedRelationalQuery other = (WrappedRelationalQuery) obj;
        return (relationalQuery.equals(other.relationalQuery));
    }
}

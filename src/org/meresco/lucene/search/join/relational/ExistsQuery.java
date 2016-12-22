package org.meresco.lucene.search.join.relational;

import java.util.Map;

import org.meresco.lucene.Lucene;


public class ExistsQuery implements RelationalQuery {
    RelationalQuery q;

    public ExistsQuery(RelationalQuery q) {
        assert q != null;
        this.q = q;
    }

    @Override
    public KeyBits collectKeys(Map<String, Lucene> lucenes) {
        return this.runner().collectKeys(lucenes);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "(" + this.q + ")";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((q == null) ? 0 : q.hashCode());
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
        ExistsQuery other = (ExistsQuery) obj;
        if (!q.equals(other.q)) {
            return false;
        }
        return true;
    }

    @Override
    public Runner runner() {
        return new Runner() {
            private Runner q = ExistsQuery.this.q.runner();
            private KeyBits filter;
            private KeyBits union;
            private boolean inverted = false;

            @Override
            public KeyBits collectKeys(Map<String, Lucene> lucenes) {
                // TODO: should this even follow the same protocol?
                throw new UnsupportedOperationException();
            }

            @Override
            public void invert() {
                this.inverted = true;
            }

            @Override
            public void filter(KeyBits keys) {
                this.filter = keys;
            }

            @Override
            public void union(KeyBits keys) {
                this.union = keys;
            }

            @Override
            public String toString() {
                return getClass().getSimpleName() + "@" + System.identityHashCode(this) + "(" + ExistsQuery.this + ")";
            }
        };
    }
}

package org.meresco.lucene.search.join.relational;

import java.util.Map;

import org.meresco.lucene.Lucene;


public class RelationalNotQuery implements RelationalQuery {
    RelationalQuery q;

    public RelationalNotQuery(RelationalQuery q) {
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
        RelationalNotQuery other = (RelationalNotQuery) obj;
        if (!q.equals(other.q)) {
            return false;
        }
        return true;
    }

    @Override
    public Runner runner() {
        return new Runner() {
            private Runner q = RelationalNotQuery.this.q.runner();
            private KeyBits filter;
            private KeyBits union;
            private boolean inverted = false;

            @Override
            public KeyBits collectKeys(Map<String, Lucene> lucenes) {
                if (!this.inverted) {
                    this.q.invert();
                }
                if (this.filter != null) {
                    this.q.filter(this.filter);
                }
                else if (this.union != null) {
                    this.q.union(this.union);
                }
                KeyBits result = this.q.collectKeys(lucenes);
                if (this.union != null) {
                    result.union(this.union);
                }
                return result;
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
                return getClass().getSimpleName() + "@" + System.identityHashCode(this) + "(" + RelationalNotQuery.this + ")";
            }
        };
    }
}

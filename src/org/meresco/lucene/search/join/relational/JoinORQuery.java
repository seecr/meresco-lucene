package org.meresco.lucene.search.join.relational;

import java.util.Map;

import org.meresco.lucene.Lucene;


public class JoinORQuery implements RelationalQuery {
    private RelationalQuery first;
    private RelationalQuery second;

    public JoinORQuery(RelationalQuery first, RelationalQuery second) {
        assert first != null && second != null;
        this.first = first;
        this.second = second;
    }

    @Override
    public KeyBits collectKeys(Map<String, Lucene> lucenes) {
        return this.runner().collectKeys(lucenes);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "(" + first + ", " + second + ")";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((first == null) ? 0 : first.hashCode());
        result = prime * result + ((second == null) ? 0 : second.hashCode());
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
        JoinORQuery other = (JoinORQuery) obj;
        if (!first.equals(other.first)) {
            return false;
        }
        if (!second.equals(other.second)) {
            return false;
        }
        return true;
    }

    @Override
    public RelationalQueryRunner runner() {
        return new RelationalQueryRunner() {
            RelationalQueryRunner first = JoinORQuery.this.first.runner();
            RelationalQueryRunner second = JoinORQuery.this.second.runner();
            KeyBits filter;
            KeyBits union;
            boolean inverted;

            @Override
            public KeyBits collectKeys(Map<String, Lucene> lucenes) {
                if (this.filter != null) {
                    this.first.filter(this.filter);
                }
                else if (!this.inverted && this.union != null) {
                    this.first.union(this.union);
                }
                KeyBits resultFirst = this.first.collectKeys(lucenes);

                if (this.filter != null) {
                    this.second.filter(this.filter);
                }
                this.second.union(resultFirst);
                KeyBits result = this.second.collectKeys(lucenes);

                if (this.inverted) {
                    result.inverted = true;
                    if (this.filter != null) {
                        this.filter.intersect(result);
//                        Utils.assertTrue(this.union == null, "union not expected (because of filter) for " + this);  // TODO: can we prove somehow that this is guaranteed to be the case?
                        return this.filter;
                    }
                    else if (this.union != null) {
                        this.union.union(result);
                        return this.union;
                    }
                }
                else {
                    if (this.union != null) {
                        result.union(this.union);
                    }
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
                return getClass().getSimpleName() + "@" + System.identityHashCode(this) + "(" + JoinORQuery.this + ")";
            }
        };
    }
}

package org.meresco.lucene.search.join.relational;

import java.util.Map;

import org.meresco.lucene.Lucene;


public class JoinANDQuery implements RelationalQuery {
    RelationalQuery first;
    RelationalQuery second;

    public JoinANDQuery(RelationalQuery first, RelationalQuery second) {
        this.first = first;
        this.second = second;
    }


    @Override
    public IntermediateResult collectKeys(Map<String, Lucene> lucenes) {
        return this.asExecutable().collectKeys(lucenes);
    }

    @Override
    public ExecutableRelationalQuery asExecutable() {
        return new ExecutableJoinANDQuery(this);
    }

    @Override
    public String toString() {
        return "JoinANDQuery(" + first + ", " + second + ")";
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
        JoinANDQuery other = (JoinANDQuery) obj;
        if (!first.equals(other.first)) {
            return false;
        }
        if (!second.equals(other.second)) {
            return false;
        }
        return true;
    }


    static class ExecutableJoinANDQuery implements ExecutableRelationalQuery {
        RelationalQuery relationalQuery;
        ExecutableRelationalQuery first;
        ExecutableRelationalQuery second;
        IntermediateResult filter;
        IntermediateResult union;
        boolean inverted;

        ExecutableJoinANDQuery(JoinANDQuery q) {
            this.relationalQuery = q;
            this.first = q.first.asExecutable();
            this.second = q.second.asExecutable();
        }

        @Override
        public IntermediateResult collectKeys(Map<String, Lucene> lucenes) {
    //        System.out.println("collectKeys " + this);
            if (this.filter != null) {
    //            System.out.println("apply filter " + this.filter + " to ANDQuery.first " + this.first);
                this.first.filter(this.filter);
            }
            IntermediateResult result = this.first.collectKeys(lucenes);
    //        System.out.println("apply filter " + result + " to ANDQuery.second " + this.second);
            this.second.filter(result);
            if (!this.inverted && this.union != null) {
    //            System.out.println("apply union " + this.union + " to ANDQuery.second " + this.second);
                this.second.union(this.union);
            }
            result = this.second.collectKeys(lucenes);

            if (this.inverted) {
                result.inverted = true;
                if (this.filter != null) {
    //                System.out.println("[JoinANDQuery] applying bitset filter " + result + " to filter " + this.filter);
                    this.filter.intersect(result);  // note: no ranking (but shouldn't be an issue)
//                    Utils.assertTrue(this.union == null, "union not expected (because of filter) for " + this); // TODO: can we prove somehow that this is guaranteed to be the case?
                    return this.filter;
                }
                else if (this.union != null) {
    //                System.out.println("[JoinANQuery] applying bitset union " + result + " to union " + this.union);
                    this.union.union(result);  // note: no ranking (but shouldn't be an issue)
                    return this.union;
                }
            }
            return result;
        }

        @Override
        public void invert() {
//            Utils.assertTrue(!this.inverted, "invert already called for " + this);
            this.inverted = true;
        }

        @Override
        public void filter(IntermediateResult intermediateResult) {
//            Utils.assertTrue(this.filter == null, "filter already set for " + this);
            this.filter = intermediateResult;
        }

        @Override
        public void union(IntermediateResult intermediateResult) {
//            Utils.assertTrue(this.union == null, "union already set for " + this);
            this.union = intermediateResult;
        }

        @Override
        public ExecutableRelationalQuery asExecutable() {
            return this;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "@" + System.identityHashCode(this) + "(" + this.relationalQuery + ")";
        }
    }
}

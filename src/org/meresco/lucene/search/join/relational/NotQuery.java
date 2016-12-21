package org.meresco.lucene.search.join.relational;

import java.util.Map;

import org.meresco.lucene.Lucene;


public class NotQuery implements RelationalQuery {
    RelationalQuery q;

    public NotQuery(RelationalQuery q) {
        this.q = q;
    }

    @Override
    public IntermediateResult collectKeys(Map<String, Lucene> lucenes) {
//        System.out.println("collectKeys " + this);
        return this.asExecutable().collectKeys(lucenes);
    }

    @Override
    public ExecutableRelationalQuery asExecutable() {
        return new ExecutableNotQuery(this);
    }

    @Override
    public String toString() {
        return "NotQuery(" + this.q + ")";
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
        NotQuery other = (NotQuery) obj;
        if (!q.equals(other.q)) {
            return false;
        }
        return true;
    }


    static class ExecutableNotQuery implements ExecutableRelationalQuery {
        private RelationalQuery relationalQuery;
        private ExecutableRelationalQuery q;
        private IntermediateResult filter;
        private IntermediateResult union;
        private boolean inverted = false;

        ExecutableNotQuery(NotQuery nq) {
            this.relationalQuery = nq;
            this.q = nq.q.asExecutable();
        }

        @Override
        public IntermediateResult collectKeys(Map<String, Lucene> lucenes) {
//            System.out.println("collectKeys " + this);
            if (!this.inverted) {
                this.q.invert();
            }
            if (this.filter != null) {
    //            System.out.println("apply filter " + this.filter + " to NotQuery.q " + this.q);
                this.q.filter(this.filter);
            }
            else if (this.union != null) {
    //            System.out.println("apply union " + this.union + " to NotQuery.q" + this.q);
                this.q.union(this.union);
            }
            IntermediateResult result = this.q.collectKeys(lucenes);
            if (this.union != null) {
    //            System.out.println("[NotQuery] applying bitset union " + this.union + " to result " + result);
                result.union(this.union);
    //            System.out.println("result: " + result);
            }
            return result;
        }

        @Override
        public void invert() {
//            Utils.assertTrue(!this.inverted, "invert already called for " + this);
            this.inverted = true;
        }

        @Override
        public void filter(IntermediateResult keyFilter) {
//            Utils.assertTrue(this.filter == null, "filter already set for " + this);
            this.filter = keyFilter;
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

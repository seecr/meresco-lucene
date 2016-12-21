package org.meresco.lucene.search.join.relational;

import java.util.Map;

import org.meresco.lucene.Lucene;


public class JoinORQuery implements RelationalQuery {
    private RelationalQuery first;
    private RelationalQuery second;

    public JoinORQuery(RelationalQuery first, RelationalQuery second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public IntermediateResult collectKeys(Map<String, Lucene> lucenes) {
        return this.asExecutable().collectKeys(lucenes);
    }

    @Override
    public ExecutableRelationalQuery asExecutable() {
        return new ExecutableJoinORQuery(this);
    }

    @Override
    public String toString() {
        return "JoinORQuery(" + first + ", " + second + ")";
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


    static class ExecutableJoinORQuery implements ExecutableRelationalQuery {
        RelationalQuery relationalQuery;
        ExecutableRelationalQuery first;
        ExecutableRelationalQuery second;
        IntermediateResult filter;
        IntermediateResult union;
        boolean inverted;

        public ExecutableJoinORQuery(JoinORQuery q) {
            this.relationalQuery = q;
            this.first = q.first.asExecutable();
            this.second = q.second.asExecutable();
        }

        @Override
        public IntermediateResult collectKeys(Map<String, Lucene> lucenes) {
    //        System.out.println("collectKeys " + this);
            if (this.filter != null) {
    //            System.out.println("apply filter " + this.filter + " to ORQuery.first " + this.first);
                this.first.filter(this.filter);
            }
            else if (!this.inverted && this.union != null) {
    //            System.out.println("apply union " + this.union + " to ORQuery.first " + this.first);
                this.first.union(this.union);
            }
            IntermediateResult resultFirst = this.first.collectKeys(lucenes);

            if (this.filter != null) {
    //            System.out.println("apply filter " + filter + " to ORQuery.second " + this.second);
                this.second.filter(this.filter);
            }
    //        System.out.println("apply union " + resultFirst + " to ORQuery.second " + this.second);
            this.second.union(resultFirst);
            IntermediateResult result = this.second.collectKeys(lucenes);

            if (this.inverted) {
                result.inverted = true;
                if (this.filter != null) {
    //                System.out.println("[JoinORQuery] applying bitset filter " + result + " to filter " + this.filter);
                    this.filter.intersect(result);
    //                System.out.println("result: " + this.filter);
//                    Utils.assertTrue(this.union == null, "union not expected (because of filter) for " + this); // TODO: can we prove somehow that this is guaranteed to be the case?
                    return this.filter;
                }
                else if (this.union != null) {
    //                System.out.println("[JoinORQuery] applying bitset union " + result + " to union " + this.union);
                    this.union.union(result);
    //                System.out.println("result: " + this.union);
                    return this.union;
                }
            }
            else {
                if (this.union != null) {
    //                System.out.println("applying bitset union " + this.union + " to result " + result);
                    result.union(this.union);
                }
            }

    //        System.out.println("result: " + result);
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

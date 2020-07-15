/* begin license *
 *
 * "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
 *
 * Copyright (C) 2016 Seecr (Seek You Too B.V.) https://seecr.nl
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

import java.util.Map;

import org.meresco.lucene.Lucene;


public class JoinOrQuery implements RelationalQuery {
    private RelationalQuery first;
    private RelationalQuery second;

    public JoinOrQuery(RelationalQuery first, RelationalQuery second) {
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
        JoinOrQuery other = (JoinOrQuery) obj;
        if (!first.equals(other.first)) {
            return false;
        }
        if (!second.equals(other.second)) {
            return false;
        }
        return true;
    }

    @Override
    public Runner runner() {
        return new Runner() {
            Runner first = JoinOrQuery.this.first.runner();
            Runner second = JoinOrQuery.this.second.runner();
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
                return getClass().getSimpleName() + "@" + System.identityHashCode(this) + "(" + JoinOrQuery.this + ")";
            }
        };
    }
}

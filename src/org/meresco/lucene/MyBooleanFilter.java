/* begin license *
 *
 * "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
 *
 * Copyright (C) 2013 Seecr (Seek You Too B.V.) http://seecr.nl
 * Copyright (C) 2013 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
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

package org.meresco.lucene;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.BitsFilteredDocIdSet;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.OpenBitSet;
import org.apache.lucene.queries.FilterClause;

/**
 * A container Filter that allows Boolean composition of Filters.
 * Filters are allocated into one of three logical constructs;
 * SHOULD, MUST NOT, MUST
 * The results Filter BitSet is constructed as follows:
 * SHOULD Filters are OR'd together
 * The resulting Filter is NOT'd with the NOT Filters
 * The resulting Filter is AND'd with the MUST Filters
 */
public class MyBooleanFilter extends Filter implements Iterable<FilterClause> {

  private final List<FilterClause> clauses = new ArrayList<FilterClause>();

  /**
   * Returns the a DocIdSetIterator representing the Boolean composition
   * of the filters that have been added.
   */
  @Override
  public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException {
    OpenBitSet res = null;
    final AtomicReader reader = context.reader();

    boolean hasShouldClauses = false;
    for (final FilterClause fc : clauses) {
      if (fc.getOccur() == Occur.SHOULD) {
        hasShouldClauses = true;
        // final DocIdSetIterator disi = getDISI(fc.getFilter(), context);
        // if (disi == null) continue;
        OpenBitSet bitSet = getBitSet(fc.getFilter(), context);
        if (res == null) {
          res = bitSet.clone();
        } else {
          res.or(bitSet);
        }
      }
    }
    if (hasShouldClauses && res == null)
      return DocIdSet.EMPTY_DOCIDSET;


    for (final FilterClause fc : clauses) {
      if (fc.getOccur() == Occur.MUST) {
        // final DocIdSetIterator disi = getDISI(fc.getFilter(), context);
        // if (disi == null) {
          // return DocIdSet.EMPTY_DOCIDSET; // no documents can match
        // }
        OpenBitSet bitSet = getBitSet(fc.getFilter(), context);
        if (res == null) {
          res = bitSet.clone();
        } else {
          res.and(bitSet);
        }
      }
    }

    return res != null ? BitsFilteredDocIdSet.wrap(res, acceptDocs) : DocIdSet.EMPTY_DOCIDSET;
  }

  private static OpenBitSet getBitSet(Filter filter, AtomicReaderContext context)
      throws IOException {
    // we dont pass acceptDocs, we will filter at the end using an additional filter
    return (OpenBitSet) filter.getDocIdSet(context, null);
  }

  /**
  * Adds a new FilterClause to the Boolean Filter container
  * @param filterClause A FilterClause object containing a Filter and an Occur parameter
  */
  public void add(FilterClause filterClause) {
    clauses.add(filterClause);
  }

  public final void add(Filter filter, Occur occur) {
    add(new FilterClause(filter, occur));
  }

  /**
  * Returns the list of clauses
  */
  public List<FilterClause> clauses() {
    return clauses;
  }

  /** Returns an iterator on the clauses in this query. It implements the {@link Iterable} interface to
   * make it possible to do:
   * <pre class="prettyprint">for (FilterClause clause : booleanFilter) {}</pre>
   */
  @Override
  public final Iterator<FilterClause> iterator() {
    return clauses().iterator();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }

    if ((obj == null) || (obj.getClass() != this.getClass())) {
      return false;
    }

    final MyBooleanFilter other = (MyBooleanFilter)obj;
    return clauses.equals(other.clauses);
  }

  @Override
  public int hashCode() {
    return 657153718 ^ clauses.hashCode();
  }

  /** Prints a user-readable version of this Filter. */
  @Override
  public String toString() {
    final StringBuilder buffer = new StringBuilder("MyBooleanFilter(");
    final int minLen = buffer.length();
    for (final FilterClause c : clauses) {
      if (buffer.length() > minLen) {
        buffer.append(' ');
      }
      buffer.append(c);
    }
    return buffer.append(')').toString();
  }
}

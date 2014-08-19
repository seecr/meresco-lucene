/* begin license *
 *
 * "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
 *
 * Copyright (C) 2013-2014 Seecr (Seek You Too B.V.) http://seecr.nl
 * Copyright (C) 2013-2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
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

package org.meresco.lucene.queries;

import java.io.IOException;
import java.util.List;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.queries.BooleanFilter;
import org.apache.lucene.queries.FilterClause;
import org.apache.lucene.search.BitsFilteredDocIdSet;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.OpenBitSet;


public class KeyBooleanFilter extends BooleanFilter {

  /**
   * Returns the a DocIdSetIterator representing the Boolean composition
   * of the filters that have been added.
   */
  @Override
  public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException {
    OpenBitSet res = null;
    List<FilterClause> clauses = this.clauses();
    boolean hasShouldClauses = false;
    for (final FilterClause fc : clauses) {
      if (fc.getOccur() == Occur.SHOULD) {
        hasShouldClauses = true;
        OpenBitSet bitSet = getBitSet(fc.getFilter(), context);
        if (res == null) {
          res = bitSet.clone();
        } else {
          res.or(bitSet);
        }
      }
    }
    if (hasShouldClauses && res == null)
      return null;

    for (final FilterClause fc : clauses) {
      if (fc.getOccur() == Occur.MUST) {
        OpenBitSet bitSet = getBitSet(fc.getFilter(), context);
        if (res == null) {
          res = bitSet.clone();
        } else {
          res.and(bitSet);
        }
      }
    }

    return res != null ? BitsFilteredDocIdSet.wrap(res, acceptDocs) : null;
  }

  private static OpenBitSet getBitSet(Filter filter, AtomicReaderContext context)
      throws IOException {
    // we dont pass acceptDocs, we will filter at the end using an additional filter
    return (OpenBitSet) filter.getDocIdSet(context, null);
  }
}

/* begin license *
 *
 * "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
 *
 * Copyright (C) 2014 Seecr (Seek You Too B.V.) http://seecr.nl
 * Copyright (C) 2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
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

package org.meresco.lucene.search;

import org.apache.lucene.util.PriorityQueue;

/** Keeps highest results, first by largest int value,
 *  then tie break by smallest ord. */
public class TopStringAndIntQueue extends PriorityQueue<TopStringAndIntQueue.StringAndValue> {

  /** Holds a single entry. */
  public static final class StringAndValue {

    /** Ordinal of the entry. */
    public String ord;

    /** Value associated with the ordinal. */
    public int value;

    /** Default constructor. */
    public StringAndValue(String ord, int value) {
      this.ord = ord;
      this.value = value;
    }
  }

  /** Sole constructor. */
  public TopStringAndIntQueue(int topN) {
    super(topN, false);
  }

  @Override
  protected boolean lessThan(StringAndValue a, StringAndValue b) {
    if (a.value < b.value) {
      return true;
    } else if (a.value > b.value) {
      return false;
    } else {
      return true;//a.ord > b.ord;
    }
  }
}
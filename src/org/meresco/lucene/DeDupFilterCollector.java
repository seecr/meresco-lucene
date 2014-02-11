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

package org.meresco.lucene;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.Map;
import java.util.HashSet;
import java.util.HashMap;
import java.util.Iterator;
import org.apache.lucene.search.Collector;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.search.Scorer;


public class DeDupFilterCollector extends Collector {
  private int currentDocBase;
  private Collector delegate;
  public String keyName;
  private NumericDocValues keyValues;
  private Map<Long, Integer> keys = new HashMap<Long, Integer>();
  private IndexReaderContext topLevelReaderContext = null;

  public DeDupFilterCollector(String keyName, Collector delegate) {
    this.delegate = delegate;
    this.keyName = keyName;
  }

  @Override
  public void setScorer(Scorer scorer) throws IOException {
    this.delegate.setScorer(scorer);
  }

  @Override
  public void collect(int doc) throws IOException {
    long key = this.keyValues.get(doc);
    if (key > 0) {
        Integer count = this.keys.get(key);
        if (count != null) {
            this.keys.put(key, count + 1);
            return;
        }
        this.keys.put(key, 1);
    }
    this.delegate.collect(doc);
  }

  @Override
  public void setNextReader(AtomicReaderContext context) throws IOException {
    if (this.topLevelReaderContext == null)
        this.topLevelReaderContext = ReaderUtil.getTopLevelContext(context);
    this.currentDocBase = context.docBase;
    this.keyValues = context.reader().getNumericDocValues(this.keyName);
    if (this.keyValues == null)
        this.keyValues = NumericDocValues.EMPTY;
    this.delegate.setNextReader(context);
  }

  @Override
  public boolean acceptsDocsOutOfOrder() {
    return this.delegate.acceptsDocsOutOfOrder();
  }

  public int countFor(int docId) throws IOException {
    List<AtomicReaderContext> leaves = this.topLevelReaderContext.leaves();
    AtomicReaderContext context = leaves.get(ReaderUtil.subIndex(docId, leaves));
    NumericDocValues docValues = context.reader().getNumericDocValues(this.keyName);
    if (docValues == null)
      return 0;
    Long key = docValues.get(docId - context.docBase);
    if (key == null)
      return 0;
    Integer count = this.keys.get(key);
    return count == null ? 0 : count;
  }
}

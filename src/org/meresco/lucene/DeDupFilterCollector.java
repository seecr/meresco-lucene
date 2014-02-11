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
import org.apache.lucene.search.FieldCache;
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
    private Map<Long, Key> keys = new HashMap<Long, Key>();
    private IndexReaderContext topLevelReaderContext = null;

    public int totalHits = 0;

    private String sortByFieldName = null;
    private NumericDocValues sortByValues;

    public DeDupFilterCollector(String keyName, Collector delegate) {
        this.delegate = delegate;
        this.keyName = keyName;
    }

    public DeDupFilterCollector(String keyName, String sortByFieldName, Collector delegate) {
        this.delegate = delegate;
        this.keyName = keyName;
        this.sortByFieldName = sortByFieldName;
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
        this.delegate.setScorer(scorer);
    }

    @Override
    public void collect(int doc) throws IOException {
        this.totalHits++;
        long keyValue = this.keyValues.get(doc);
        if (keyValue > 0) {
            int absDoc = this.currentDocBase + doc;
            long sortByValue = this.sortByValues.get(doc);
            Key key = this.keys.get(keyValue);
            if (key != null) {
                key.count++;
                if (key.sortByValue < sortByValue) {
                    key.sortByValue = sortByValue;
                    key.docId = absDoc;
                }
                return;
            }
            this.keys.put(keyValue, new Key(absDoc, sortByValue, 1));
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

        this.sortByValues = null;
        if (this.sortByFieldName != null)
            this.sortByValues = context.reader().getNumericDocValues(this.sortByFieldName);
        if (this.sortByValues == null)
            this.sortByValues = NumericDocValues.EMPTY;

        this.delegate.setNextReader(context);
    }

    @Override
    public boolean acceptsDocsOutOfOrder() {
        return this.delegate.acceptsDocsOutOfOrder();
    }

    public Key keyForDocId(int docId) throws IOException {
        List<AtomicReaderContext> leaves = this.topLevelReaderContext.leaves();
        AtomicReaderContext context = leaves.get(ReaderUtil.subIndex(docId, leaves));
        NumericDocValues docValues = context.reader().getNumericDocValues(this.keyName);
        if (docValues == null)
            return null;
        long keyValue = docValues.get(docId - context.docBase);
        if (keyValue == 0)
            return null;
        Key key = this.keys.get(keyValue);
        if (key == null)
            return null;
        return key;
    }

    public class Key {
        public int docId;
        private long sortByValue;
        public int count;

        public Key(int docId, long sortByValue, int count) {
            this.docId = docId;
            this.sortByValue = sortByValue;
            this.count = count;
        }
    }
}

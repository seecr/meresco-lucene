/* begin license *
 *
 * "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
 *
 * Copyright (C) 2015 Koninklijke Bibliotheek (KB) http://www.kb.nl
 * Copyright (C) 2015 Seecr (Seek You Too B.V.) http://seecr.nl
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

import gnu.trove.map.hash.TLongObjectHashMap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.Scorer;

public class GroupSuperCollector extends SuperCollector<GroupSubCollector> {
    private final String keyName;
    private final SuperCollector<?> delegate;
    private IndexReaderContext topLevelReaderContext = null;

    public GroupSuperCollector(String keyName, SuperCollector<?> delegate) {
        super();
        this.keyName = keyName;
        this.delegate = delegate;
    }

    public String getKeyName() {
        return this.keyName;
    }

    @Override
    protected GroupSubCollector createSubCollector() throws IOException {
        SubCollector delegateSubCollector = this.delegate.subCollector();
        return new GroupSubCollector(this.keyName, delegateSubCollector, this);
    }

    @Override
    public void complete() throws IOException {
        this.delegate.complete();
    }

    private Long getKeyForDocId(int docId) throws IOException {
        if (this.topLevelReaderContext == null)
            this.topLevelReaderContext = ReaderUtil.getTopLevelContext(super.subs.get(0).context);

        List<AtomicReaderContext> leaves = this.topLevelReaderContext.leaves();
        AtomicReaderContext context = leaves.get(ReaderUtil.subIndex(docId, leaves));
        NumericDocValues docValues = context.reader().getNumericDocValues(this.keyName);
        if (docValues == null)
            return null;
        return docValues.get(docId - context.docBase);
    }

    public List<Integer> group(int docId) throws IOException {
        Long keyValue = this.getKeyForDocId(docId);
        if (keyValue == null) {
            return null;
        }
        List<Integer> result = new ArrayList<Integer>();
        for (GroupSubCollector sub : this.subs) {
            sub.group(keyValue, result);
        }
        return result;
    }
}

class GroupSubCollector extends SubCollector {
    private final SubCollector delegate;
    private final String keyName;
    private NumericDocValues keyValues;
    AtomicReaderContext context;
    private TLongObjectHashMap<int []> keyToDocIds;
    private GroupSuperCollector groupSuperCollector;
    
    GroupSubCollector(String keyName, SubCollector delegate, GroupSuperCollector groupSuperCollector) {
        this.keyName = keyName;
        this.delegate = delegate;
        this.groupSuperCollector = groupSuperCollector;
    }

    public void group(long keyValue, List<Integer> result) {
        int[] docIds = this.keyToDocIds.get(keyValue);
        if (docIds == null) {
            return;
        }
        for (int docId : docIds)
            if (docId != 0)
                result.add(docId == -1 ? 0 : docId);
    }

    @Override
    public void setNextReader(AtomicReaderContext context) throws IOException {
        if (this.keyToDocIds == null) {
            float loadFactor = 0.75f;
            int maxDoc = (int) (ReaderUtil.getTopLevelContext(context).reader().maxDoc() * (1 + (1 - loadFactor)));
            this.keyToDocIds = new TLongObjectHashMap<int []>(maxDoc / this.groupSuperCollector.subs.size(), loadFactor);
        }
        this.context = context;
        this.delegate.setNextReader(context);
        NumericDocValues kv = context.reader().getNumericDocValues(this.keyName);
        if (kv == null)
            kv = DocValues.emptyNumeric();
        this.keyValues = kv;
        this.delegate.setNextReader(context);
    }

    @Override
    public void collect(int doc) throws IOException {
        long keyValue = this.keyValues.get(doc);
        if (keyValue > 0) {
            int[] docIds = this.keyToDocIds.get(keyValue);
            int i = 0;
            if (docIds == null) {
                docIds = new int[2];
                this.keyToDocIds.put(keyValue, docIds);
            } else {
                for (i = 0; i < docIds.length; i++) {
                    if (docIds[i] == 0) {
                        break;
                    }
                }
                if (i == docIds.length) {
                    int[] newDocIds = new int[docIds.length * 2];
                    System.arraycopy(docIds, 0, newDocIds, 0, docIds.length);
                    docIds = newDocIds;
                    this.keyToDocIds.put(keyValue, docIds);
                }
            }
            int docId = doc + this.context.docBase;
            docIds[i] = docId == 0 ? -1 : docId;
        }
        this.delegate.collect(doc);
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
        this.delegate.setScorer(scorer);
    }

    @Override
    public boolean acceptsDocsOutOfOrder() {
        return this.delegate.acceptsDocsOutOfOrder();
    }

    @Override
    public void complete() throws IOException {
        this.delegate.complete();
    }
}
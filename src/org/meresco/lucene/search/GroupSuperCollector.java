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

import java.io.IOException;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexReaderContext;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

public class GroupSuperCollector extends SuperCollector<GroupSubCollector> {
    private final String keyName;
    private final SuperCollector<?> delegate;
    private IndexReaderContext topLevelReaderContext = null;

    public GroupSuperCollector(String keyName, SuperCollector<?> delegate) {
        super();
        this.keyName = keyName;
        this.delegate = delegate;
    }

    @Override
    protected GroupSubCollector createSubCollector() throws IOException {
        SubCollector delegateSubCollector = this.delegate.subCollector();
        return new GroupSubCollector(this.keyName, delegateSubCollector);
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

    public List<Integer> group(int docId) throws IOException{
        Long keyValue = this.getKeyForDocId(docId);
        if (keyValue == null) {
            return null;
        }
        List result = new ArrayList<Integer>();
        for(GroupSubCollector sub : this.subs){
            sub.group(keyValue, result);
        }
        return result;
    }

}

class GroupSubCollector extends SubCollector{
    private final SubCollector delegate;
    private final String keyName;
    private NumericDocValues keyValues;
    AtomicReaderContext context;
    private Map<Long, List<Integer>> keyToDocIds = new HashMap<Long, List<Integer>>();


    GroupSubCollector(String keyName, SubCollector delegate){
        this.keyName = keyName;
        this.delegate = delegate;
    }

    public void group(long keyValue, List<Integer> result){
        List<Integer> docIds = this.keyToDocIds.get(keyValue);
        if(docIds == null){
            return;
        }
        result.addAll(docIds);
    }

    @Override
    public void setNextReader(AtomicReaderContext context) throws IOException {
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
        if (keyValue > 0){
            List<Integer> docIds = this.keyToDocIds.get(keyValue);
            if(docIds == null) {
                docIds = new ArrayList<Integer>();
                this.keyToDocIds.put(keyValue, docIds);
            }
            docIds.add(doc + this.context.docBase);
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
package org.meresco.lucene.search;

import java.io.IOException;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;

public class TopDocSubCollector<SuperCollectorType extends TopDocSuperCollector> extends
        DelegatingSubCollector<TopDocsCollector<?>, SuperCollectorType> {

    TopDocs topdocs;

    public TopDocSubCollector(TopDocsCollector<?> docCollector, SuperCollectorType parent) throws IOException {
        super(docCollector, parent);
    }

    @Override
    public void complete() {
        this.topdocs = this.delegate.topDocs();
    }
}
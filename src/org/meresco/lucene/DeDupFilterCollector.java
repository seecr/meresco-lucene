package org.meresco.lucene;
import java.io.IOException;
import org.apache.lucene.search.Collector;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Scorer;

public class DeDupFilterCollector extends Collector {

  private Collector next;

  public DeDupFilterCollector(String keyName, Collector next) {
    this.next = next;    
  }

  @Override
  public void setScorer(Scorer scorer) throws IOException {
    this.next.setScorer(scorer);
  }

  @Override
  public void collect(int doc) throws IOException {
    this.next.collect(doc);
  }

  @Override
  public void setNextReader(AtomicReaderContext context) throws IOException {
    this.next.setNextReader(context);
  }

  @Override
  public boolean acceptsDocsOutOfOrder() {
    return this.next.acceptsDocsOutOfOrder();
  }
}

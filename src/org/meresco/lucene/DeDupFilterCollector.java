package org.meresco.lucene;
import java.io.IOException;
import java.util.Set;
import java.util.HashSet;
import org.apache.lucene.search.Collector;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.Scorer;

public class DeDupFilterCollector extends Collector {

  private Collector next;
  private String keyName;
  private NumericDocValues keyValues;
  private Set<Long> keySet = new HashSet<Long>();

  public DeDupFilterCollector(String keyName, Collector next) {
    this.next = next;    
    this.keyName = keyName;
  }

  public DeDupFilterCollector(String keyName) {
    this(keyName, null);
  }

  public void setDelegate(Collector next) {
    this.next = next;
  }

  @Override
  public void setScorer(Scorer scorer) throws IOException {
    this.next.setScorer(scorer);
  }

  @Override
  public void collect(int doc) throws IOException {
    long key = this.keyValues.get(doc);
    if (key > 0) {
        if (this.keySet.contains(key))
            return;
        this.keySet.add(key);
    }
    this.next.collect(doc);
  }

  @Override
  public void setNextReader(AtomicReaderContext context) throws IOException {
    this.keyValues = context.reader().getNumericDocValues(this.keyName);
    if (this.keyValues == null)
        this.keyValues = NumericDocValues.EMPTY;
    this.next.setNextReader(context);
  }

  @Override
  public boolean acceptsDocsOutOfOrder() {
    return this.next.acceptsDocsOutOfOrder();
  }
}

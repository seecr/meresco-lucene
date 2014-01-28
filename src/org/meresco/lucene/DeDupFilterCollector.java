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
import org.apache.lucene.search.Scorer;


public class DeDupFilterCollector extends Collector {
  private int currentDocBase;
  private Collector delegate;
  private String keyName;
  private NumericDocValues keyValues;
  private Map<Long, Integer> key2UnskippedDoc = new HashMap<Long, Integer>();
  private Set<Integer> similarFiltered = new HashSet<Integer>();

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
        if (this.key2UnskippedDoc.containsKey(key)) {
            int unskippedDoc = this.key2UnskippedDoc.get(key);
            this.similarFiltered.add(unskippedDoc);
            return;
        }
        this.key2UnskippedDoc.put(key, this.currentDocBase + doc);
    }
    this.delegate.collect(doc);
  }

  @Override
  public void setNextReader(AtomicReaderContext context) throws IOException {
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

  public int[] similarFiltered() {
    int size = this.similarFiltered.size();
    int[] result = new int[size];

    Iterator<Integer> iterator = this.similarFiltered.iterator();
    for (int i=0; i < size; i++) {
        result[i] = iterator.next();
    }

    return result;
  }
}

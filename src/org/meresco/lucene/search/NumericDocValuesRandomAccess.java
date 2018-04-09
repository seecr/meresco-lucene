package org.meresco.lucene.search;

import java.io.IOException;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;


/**
 * Wraps a NumericDocValues instance (as read from a LeafReader by name) to provide old-style random access the way we depend on it.
 *
 * TODO: verify that we really need it and measure performance penalty.
 */
public class NumericDocValuesRandomAccess {
    private LeafReader reader;
    private String name;
    private NumericDocValues numericDocValues;
    private int lastDocID = Integer.MAX_VALUE;

    public NumericDocValuesRandomAccess(LeafReader reader, String name) {
        this.reader = reader;
        this.name = name;
    }

    public long get(int docID) {
        try {
            if (docID < this.lastDocID && this.name != null) {
                if (lastDocID != Integer.MAX_VALUE) {
                    System.out.println("DEBUG reread needed, because non-ascending access: " + lastDocID + ", " + docID);
                }
                this.numericDocValues = this.reader.getNumericDocValues(this.name);
            }
            if (this.numericDocValues == null || !this.numericDocValues.advanceExact(docID)) {
                return 0;  // mimics behaviour of e.g. the old DocValues.emptyNumeric()
            }
            this.lastDocID = docID;
            return this.numericDocValues.longValue();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

package org.meresco.lucene.search;

import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.ml.clustering.Clusterable;
import org.apache.commons.math3.util.OpenIntToDoubleHashMap;
import org.apache.commons.math3.util.OpenIntToDoubleHashMap.Iterator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;

class MerescoVector implements Clusterable {
    private OpenIntToDoubleHashMap entries;
    int docId;
    private int maxIndex;
    private ArrayRealVector point = null;

    public MerescoVector(int docId) {
        this.entries = new OpenIntToDoubleHashMap(0.0);
        this.docId = docId;
        this.maxIndex = 0;
    }

    public MerescoVector() {
        this(-1);
    }

    public void setEntry(int index, double value) {
        this.entries.put(index, value);
        if (index > this.maxIndex)
            this.maxIndex = index;
    }

    public void combineToSelf(double a, double b, MerescoVector y) {
        int maxIndex = Math.max(this.maxIndex, y.maxIndex);
        for (int i = 0; i <= maxIndex; i++) {
            final double xi = this.entries.get(i);
            final double yi = y.entries.get(i);
            setEntry(i, a * xi + b * yi);
        }
    }

    public double[] getPoint() {
        if (this.point == null) {
            this.point = new ArrayRealVector(this.maxIndex + 1);
            Iterator iter = entries.iterator();
            while (iter.hasNext()) {
                iter.advance();
                this.point.setEntry(iter.key(), iter.value());
            }
        }
        this.point.unitize();
        return this.point.getDataRef();
    }

    public int docId() {
        return this.docId;
    }

    public void printVector(BytesRefHash hash) {
        Iterator iter = entries.iterator();
        while (iter.hasNext()) {
            iter.advance();
            if (iter.value() > 0) {
                BytesRef b = new BytesRef();
                hash.get(iter.key(), b);
                System.out.print(b.utf8ToString() + ":" + iter.value() + "  ");
            }
        }
        System.out.println();
    }
}
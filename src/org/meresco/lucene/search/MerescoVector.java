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
    private BytesRefHash ords;
    private BytesRefHash frequentOrds;

    public MerescoVector(int docId, BytesRefHash ords, BytesRefHash frequentOrds) {
        this.entries = new OpenIntToDoubleHashMap(0.0);
        this.docId = docId;
        this.maxIndex = 0;
        this.ords = ords;
        this.frequentOrds = frequentOrds;
    }

    public MerescoVector() {
        this(-1, null, null);
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

    @Override
    public double[] getPoint() {
        if (this.point == null) {
            this.point = new ArrayRealVector(this.frequentOrds.size());
            Iterator iter = entries.iterator();
            BytesRef ref = new BytesRef();
            while (iter.hasNext()) {
                iter.advance();
                this.ords.get(iter.key(), ref);
                int ord = this.frequentOrds.find(ref);
                if (ord >= 0)
                    this.point.setEntry(ord, iter.value());
            }
        }
        if (this.point.getMaxValue() > 0.0)
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
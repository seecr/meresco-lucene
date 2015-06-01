package org.meresco.lucene.search;

import java.util.Map;

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
    private Map<Integer, Integer> counts;
    private Map<Integer, Integer> freqs;
    private int numDocs;
    private int numHits;

    public MerescoVector(int docId, int numDocs, int numHits, BytesRefHash ords, Map<Integer, Integer> counts, Map<Integer, Integer> freqs) {
        this.entries = new OpenIntToDoubleHashMap(0.0);
        this.docId = docId;
        this.maxIndex = 0;
        this.ords = ords;
        this.counts = counts;
        this.freqs = freqs;
        this.numDocs = numDocs;
        this.numHits = numHits;
        if (numHits == 0)
            throw new RuntimeException("0");
    }

    public MerescoVector() {
        this(-1, 0, 0, null, null, null);
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
            this.point = new ArrayRealVector(this.ords.size());
            Iterator iter = entries.iterator();
            double i = 0;
            while (iter.hasNext()) {
                iter.advance();
                int ord = iter.key();
                double p_x = this.freqs.get(ord) / (double) this.numDocs;
                double p_y = this.numHits / (double) this.numDocs;
                double p_x_y = this.counts.get(ord) / (double) this.ords.size();
                double mi = p_x_y * Math.log(p_x_y / (p_x * p_y));
                BytesRef br = new BytesRef();
                this.ords.get(ord, br);
                if (mi > 0.05) {
                    i++;
                }
                System.out.println("MI (" + br.utf8ToString() + ") = " + mi);
                this.point.setEntry(ord, iter.value());
            }
            System.out.println(i + "/" + this.entries.size() + "=" + i / entries.size());
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
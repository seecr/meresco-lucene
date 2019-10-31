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

import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.ml.clustering.Clusterable;
import org.apache.commons.math3.util.OpenIntToDoubleHashMap;
import org.apache.commons.math3.util.OpenIntToDoubleHashMap.Iterator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;

public class MerescoVector implements Clusterable {
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

	@Override
	public String toString() {
		return "MerescoVector [docId=" + docId + "]";
	}
    
    
}
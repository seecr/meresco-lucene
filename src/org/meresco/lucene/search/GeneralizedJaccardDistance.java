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

import org.apache.commons.math3.ml.distance.DistanceMeasure;
import org.apache.commons.math3.util.FastMath;

public class GeneralizedJaccardDistance implements DistanceMeasure {

    private static final long serialVersionUID = -1340861619355236388L;

    // static int n = 0;

    @Override
    public double compute(final double[] a, final double[] b) {
        final double[] X = a.length < b.length ? a : b;
        final double[] Y = X == a ? b : a;

        double sum_min = 0.0;
        double sum_max = 0.0;
        int i = 0;
        for (; i < X.length; i++) {
            final double x = X[i];
            final double y = Y[i];
            sum_min += FastMath.min(x, y);
            sum_max += FastMath.max(x, y);
        }
        for (i = i + 1; i < Y.length; i++)
            sum_max += Y[i];
        return 1.0 - sum_min / sum_max;
    }
}

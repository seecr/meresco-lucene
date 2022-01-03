/* begin license *
 *
 * "Meresco Lucene" is a set of components and tools to integrate Lucene into Meresco
 *
 * Copyright (C) 2015-2016 Koninklijke Bibliotheek (KB) http://www.kb.nl
 * Copyright (C) 2015-2016, 2021-2022 Seecr (Seek You Too B.V.) https://seecr.nl
 * Copyright (C) 2016 Stichting Kennisnet http://www.kennisnet.nl
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

package org.meresco.lucene.py_analysis;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.DelegatingAnalyzerWrapper;

public class DelegatingAnalyzer extends DelegatingAnalyzerWrapper {

    // Plz use meresco.lucene.py_analysis.TextField when possible

    private int position_gap;
    private Analyzer delegate;

    public DelegatingAnalyzer(Analyzer delegate, int position_gap) {
        super(delegate.getReuseStrategy());
        this.position_gap = position_gap;
        this.delegate = delegate;
    };

    @Override
    public Analyzer getWrappedAnalyzer(String name) {
        return this.delegate;
    };

    @Override
    public int getPositionIncrementGap(String fieldName) {
        return this.position_gap;
    };
}

/* begin license *
 *
 * "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
 *
 * Copyright (C) 2018 Seecr (Seek You Too B.V.) https://seecr.nl
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

/* begin license *
 *
 * "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
 *
 * Copyright (C) 2014, 2016 Seecr (Seek You Too B.V.) http://seecr.nl
 * Copyright (C) 2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
 * Copyright (C) 2016 Koninklijke Bibliotheek (KB) http://www.kb.nl
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

package org.meresco.lucene.test;

import java.io.IOException;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.LeafMetaData;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.Terms;
import org.apache.lucene.util.Bits;


public class DummyIndexReader {
    public static LeafReader dummyIndexReader(final int maxDoc) {
        return new LeafReader() {
            @Override
            public int maxDoc() {
                return maxDoc;
            }

            @Override
            public int numDocs() {
                return maxDoc;
            }

            @Override
            public FieldInfos getFieldInfos() {
                return new FieldInfos(new FieldInfo[0]);
            }

            @Override
            public Bits getLiveDocs() {
                return null;
            }

            @Override
            public Fields getTermVectors(int doc) {
                return null;
            }

            @Override
            public NumericDocValues getNumericDocValues(String field) {
                return null;
            }

            @Override
            public BinaryDocValues getBinaryDocValues(String field) {
                return null;
            }

            @Override
            public SortedDocValues getSortedDocValues(String field) {
                return null;
            }

            @Override
            public SortedNumericDocValues getSortedNumericDocValues(String field) {
                return null;
            }

            @Override
            public SortedSetDocValues getSortedSetDocValues(String field) {
                return null;
            }

            @Override
            public NumericDocValues getNormValues(String field) {
                return null;
            }

            @Override
            protected void doClose() {
            }

            @Override
            public void document(int doc, StoredFieldVisitor visitor) {
            }

            @Override
            public void checkIntegrity() throws IOException {
            }

            @Override
            public PointValues getPointValues(String field) {
                return null;
            }

            @Override
            public CacheHelper getCoreCacheHelper() {
                return null;
            }

            @Override
            public Terms terms(String field) throws IOException {
                return null;
            }

            @Override
            public LeafMetaData getMetaData() {
                return null;
            }

            @Override
            public CacheHelper getReaderCacheHelper() {
                return null;
            }
        };
    }
}
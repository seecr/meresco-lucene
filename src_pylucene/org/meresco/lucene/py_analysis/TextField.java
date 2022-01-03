/* begin license *
 *
 * "Meresco Lucene" is a set of components and tools to integrate Lucene into Meresco
 *
 * Copyright (C) 2021-2022 Seecr (Seek You Too B.V.) https://seecr.nl
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

import java.io.IOException;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.document.Field;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;

public class TextField implements IndexableField {
    // A analyzed field that separates different values as to keep spanqueries from mixing
    // results from different values (sections, paragraphs). Piet Pietersen problem.
    // It works exactly like getPositionIncrementGap on Analyzer (see IndexingChain), but
    // dynamically, without the need for pre-configuring an Analyzer.
    // Also, if allows for a different Analyser to be thrown in dynamically, for example
    // a language tag might cause a language-specific analyzer.
    //
    // NB: this is performance sensitive code !!
    //

    org.apache.lucene.document.TextField delegate;
    int position_gap;
    Analyzer analyzer;

    public TextField(String name, String value, Field.Store store, int position_gap, Analyzer analyzer) {
        this.delegate = new org.apache.lucene.document.TextField(name, value, store);
        this.position_gap = position_gap;
        this.analyzer = analyzer;
    };

    // See IndexingChain.invert() for when getPositionIncrement is called
    @Override
    public TokenStream tokenStream(Analyzer analyzer, TokenStream reuse) {
        Analyzer ana = this.analyzer != null ? this.analyzer : analyzer;
        TokenStream stream = this.delegate.tokenStream(ana, reuse);

        // Create or get ref to existing attribute
        PositionIncrementAttribute pia = stream.addAttribute(PositionIncrementAttribute.class);

        return new TokenStream(stream) {
            @Override
            public final boolean incrementToken() throws IOException {
                return stream.incrementToken();
            };
            @Override
            public void end() throws IOException {
                super.end();
                stream.end();
                // When done, we feed the additional position gap, see IndexingChain
                pia.setPositionIncrement(position_gap);
            };
            @Override
            public void reset() throws IOException {
                super.reset();
                stream.reset();
            };
            @Override
            public void close() throws IOException {
                super.close();
                stream.close();
            };
        };
    };

    // just delegation below

    @Override
    public BytesRef binaryValue() {
        return this.delegate.binaryValue();
    };

    @Override
    public String stringValue() {
        return this.delegate.stringValue();
    };

    @Override
    public IndexableFieldType fieldType() {
        return this.delegate.fieldType();
    };

    @Override
    public CharSequence getCharSequenceValue() {
        return this.getCharSequenceValue();
    };

    @Override
    public String name() {
        return this.delegate.name();
    };

    @Override
    public java.io.Reader readerValue() {
        return this.delegate.readerValue();
    };

    @Override
    public Number numericValue() {
        return this.delegate.numericValue();
    };

}

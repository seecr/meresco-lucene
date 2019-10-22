/* begin license *
 *
 * "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
 *
 * Copyright (C) 2016 Koninklijke Bibliotheek (KB) http://www.kb.nl
 * Copyright (C) 2016 Seecr (Seek You Too B.V.) http://seecr.nl
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

package org.meresco.lucene.suggestion;

import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.ngram.NGramTokenFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

class NGramAnalyzer extends Analyzer {
    private int minShingleSize;
    private int maxShingleSize;

    public NGramAnalyzer(int minShingleSize, int maxShingleSize) {
        this.minShingleSize = minShingleSize;
        this.maxShingleSize = maxShingleSize;
    }

    @Override
    protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer source = new StandardTokenizer();
        TokenStream src = new LowerCaseFilter(source);
        src = new AddWordBoundaryFilter(src);
        NGramTokenFilter filter = new NGramTokenFilter(src, this.minShingleSize, this.maxShingleSize, false);
        return new TokenStreamComponents(source, filter);
    }

    private class AddWordBoundaryFilter extends TokenFilter {
        private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);

        public AddWordBoundaryFilter(TokenStream in) {
            super(in);
        }

        @Override
        public final boolean incrementToken() throws IOException {
            if (!this.input.incrementToken())
                return false;

            int length = this.termAtt.length();
            char[] newBuffer = new char[length + 2];
            newBuffer[0] = '$';
            System.arraycopy(this.termAtt.buffer(), 0, newBuffer, 1, length);
            newBuffer[length + 1] = '$';

            this.termAtt.copyBuffer(newBuffer, 0, newBuffer.length);
            return true;
        }
    }
}

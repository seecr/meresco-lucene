package org.meresco.lucene.suggestion;

import java.io.IOException;
import java.io.Reader;

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
    protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
        Tokenizer source = new StandardTokenizer(reader);
        TokenStream src = new LowerCaseFilter(source);
        src = new AddWordBoundaryFilter(src);
        NGramTokenFilter filter = new NGramTokenFilter(src, this.minShingleSize, this.maxShingleSize);
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

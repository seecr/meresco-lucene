package org.meresco.lucene.suggestion;

import java.io.Reader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.shingle.ShingleFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;

class ShingleAnalyzer extends Analyzer {
    private int minShingleSize;
    private int maxShingleSize;

    public ShingleAnalyzer(int minShingleSize, int maxShingleSize) {
        this.minShingleSize = minShingleSize;
        this.maxShingleSize = maxShingleSize;
    }

    @Override
    protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
        Tokenizer source = new StandardTokenizer(reader);
        TokenStream src = new LowerCaseFilter(source);
        ShingleFilter filter = new ShingleFilter(src, this.minShingleSize, this.maxShingleSize);
        return new TokenStreamComponents(source, filter);
    }
}

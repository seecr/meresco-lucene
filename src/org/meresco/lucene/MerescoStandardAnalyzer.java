package org.meresco.lucene;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.miscellaneous.ASCIIFoldingFilter;
import org.apache.lucene.analysis.standard.ClassicFilter;
import org.apache.lucene.analysis.standard.ClassicTokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.util.Version;

public class MerescoStandardAnalyzer extends Analyzer {
    private Version matchVersion;

    public MerescoStandardAnalyzer(Version matchVersion) {
        this.matchVersion = matchVersion;
    }
    public Analyzer.TokenStreamComponents createComponents(String fieldName, java.io.Reader reader) {
        final ClassicTokenizer src = new ClassicTokenizer(this.matchVersion, reader);
        TokenStream tok = new ClassicFilter(src);
        tok = new LowerCaseFilter(this.matchVersion, tok);
        tok = new ASCIIFoldingFilter(tok);
        return new Analyzer.TokenStreamComponents(src, tok);
    }
}

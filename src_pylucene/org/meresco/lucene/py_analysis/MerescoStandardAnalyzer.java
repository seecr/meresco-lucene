/* begin license *
 *
 * "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
 *
 * Copyright (C) 2013-2016 Seecr (Seek You Too B.V.) http://seecr.nl
 * Copyright (C) 2013-2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
 * Copyright (C) 2015-2016 Koninklijke Bibliotheek (KB) http://www.kb.nl
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

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.miscellaneous.ASCIIFoldingFilter;
import org.apache.lucene.analysis.standard.ClassicFilter;
import org.apache.lucene.analysis.standard.ClassicTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

public class MerescoStandardAnalyzer {

    public List<String> pre_analyse(String fieldName, String string) throws IOException {
        final ClassicTokenizer src = new ClassicTokenizer(new StringReader(string));
        TokenStream tok = new ClassicFilter(src);
        tok = new ASCIIFoldingFilter(tok);
        tok = new LowerCaseFilter(tok);
        return this.readTokenStream(tok);
    }

    public List<String> post_analyse(String fieldName, String string) throws IOException {
        ClassicTokenizer src = new ClassicTokenizer(new StringReader(string));
        src.setReader(new StringReader(string));
        TokenStream tok = this.post_analyzer(fieldName, src);
        return this.readTokenStream(tok);
    }

    protected TokenStream post_analyzer(String fieldName, TokenStream tok) {
        return tok;
    }

    public static List<String> readTokenStream(TokenStream tok) throws IOException {
        List<String> terms = new ArrayList<String>();
        CharTermAttribute termAtt = tok.addAttribute(CharTermAttribute.class);
        try {
            tok.reset();
            while( tok.incrementToken()) {
                terms.add(termAtt.toString());
            }
            tok.end();
        } finally {
            tok.close();
        }
        return terms;
    }
}

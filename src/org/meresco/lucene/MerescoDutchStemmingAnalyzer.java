/* begin license *
 *
 * "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
 *
 * Copyright (C) 2013 Seecr (Seek You Too B.V.) http://seecr.nl
 * Copyright (C) 2013 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
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

package org.meresco.lucene;

import java.io.StringReader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.miscellaneous.ASCIIFoldingFilter;
import org.apache.lucene.analysis.standard.ClassicFilter;
import org.apache.lucene.analysis.standard.ClassicTokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.util.Version;

import org.apache.lucene.analysis.snowball.SnowballFilter;
import org.tartarus.snowball.ext.DutchStemmer;


public class MerescoDutchStemmingAnalyzer extends Analyzer {
    private Version matchVersion;

    public MerescoDutchStemmingAnalyzer(Version matchVersion) {
        this.matchVersion = matchVersion;
    }
    public Analyzer.TokenStreamComponents createComponents(String fieldName, java.io.Reader reader) {
        final ClassicTokenizer src = new ClassicTokenizer(this.matchVersion, reader);
        TokenStream tok = new ClassicFilter(src);
        tok = new ASCIIFoldingFilter(tok);
        tok = new LowerCaseFilter(this.matchVersion, tok);

        tok = new SnowballFilter(tok, new DutchStemmer());
        
        return new Analyzer.TokenStreamComponents(src, tok);
    }
}

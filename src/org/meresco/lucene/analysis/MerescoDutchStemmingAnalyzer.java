/* begin license *
 *
 * "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
 *
 * Copyright (C) 2013-2015 Seecr (Seek You Too B.V.) http://seecr.nl
 * Copyright (C) 2013-2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
 * Copyright (C) 2015 Koninklijke Bibliotheek (KB) http://www.kb.nl
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

package org.meresco.lucene.analysis;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.miscellaneous.KeywordRepeatFilter;
import org.apache.lucene.analysis.miscellaneous.RemoveDuplicatesTokenFilter;
import org.apache.lucene.analysis.snowball.SnowballFilter;
import org.tartarus.snowball.ext.DutchStemmer;
import java.util.Arrays;
import java.util.List;

public class MerescoDutchStemmingAnalyzer extends MerescoStandardAnalyzer {

    private List<String> stemmingFields;

    public MerescoDutchStemmingAnalyzer() {
        this.stemmingFields = null;
    }

    public MerescoDutchStemmingAnalyzer(String[] stemmingFields) {
        this.stemmingFields = Arrays.asList(stemmingFields);
    }

    @Override
    public TokenStream post_analyzer(String fieldName, TokenStream tok) {
        if (stemmingFields != null && stemmingFields.indexOf(fieldName) == -1)
            return tok;
        tok = new KeywordRepeatFilter(tok); // repeat every word as term and as keyword
        tok = new SnowballFilter(tok, new DutchStemmer()); // ignores keywords
        tok = new RemoveDuplicatesTokenFilter(tok); // removes one if keyword and term are still the same
        return tok;
    }
}

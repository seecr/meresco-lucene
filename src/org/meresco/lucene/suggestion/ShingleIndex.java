/* begin license *
 *
 * "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
 *
 * Copyright (C) 2015 Koninklijke Bibliotheek (KB) http://www.kb.nl
 * Copyright (C) 2015 Seecr (Seek You Too B.V.) http://seecr.nl
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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.shingle.ShingleFilter;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.ngram.NGramTokenFilter;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

import java.util.List;
import java.util.ArrayList;
import java.io.Reader;
import java.io.IOException;
import java.io.File;

public class ShingleIndex {

    private IndexWriter writer;
    private DirectoryReader reader;
    private IndexSearcher searcher;
    private ShingleAnalyzer findShingles;
    private ShingleAnalyzer shingleAnalyzer;
    private NGramAnalyzer bigram;
    private NGramAnalyzer trigram;

    private static final String SHINGLE_FIELDNAME = "__shingle__";
    private static final String BIGRAM_FIELDNAME = "__bigram__";
    private static final String TRIGRAM_FIELDNAME = "__trigram__";

    public ShingleIndex(String directory, int minShingleSize, int maxShingleSize) throws IOException {
        this.shingleAnalyzer = new ShingleAnalyzer(minShingleSize, maxShingleSize);
        this.bigram = new NGramAnalyzer(2, 2);
        this.trigram = new NGramAnalyzer(3, 3);

        Directory dir = FSDirectory.open(new File(directory));
        IndexWriterConfig config = new IndexWriterConfig(Version.LATEST, new StandardAnalyzer());
        this.writer = new IndexWriter(dir, config);
        this.writer.commit();
        this.reader = DirectoryReader.open(dir);
        this.searcher = new IndexSearcher(this.reader);
    }

    public void add(String value) throws IOException {
        for (String shingle : shingles(value)) {
            Document doc = new Document();
            doc.add(new StringField(SHINGLE_FIELDNAME, shingle, Field.Store.YES));
            for (String n : ngrams(shingle, false)) {
                doc.add(new StringField(BIGRAM_FIELDNAME, n, Field.Store.NO));
            }
            for (String n : ngrams(shingle, true)) {
                doc.add(new StringField(TRIGRAM_FIELDNAME, n, Field.Store.NO));
            }
            this.writer.updateDocument(new Term(SHINGLE_FIELDNAME, shingle), doc);
        }
        this.writer.commit();
    }

    public String[] suggest(String value, Boolean trigram) throws IOException {
        DirectoryReader newReader = DirectoryReader.openIfChanged(this.reader);
        if (newReader != null) {
            this.reader = newReader;
            this.searcher = new IndexSearcher(this.reader);
        }

        String ngramFieldName = trigram ? TRIGRAM_FIELDNAME : BIGRAM_FIELDNAME;
        BooleanQuery query = new BooleanQuery();
        for (String n : ngrams(value, trigram)) {
            query.add(new TermQuery(new Term(ngramFieldName, n)), BooleanClause.Occur.MUST);
        }
        TopDocs t = this.searcher.search(query, 100);
        String[] suggestions = new String[t.totalHits < 100 ? t.totalHits : 100];
        int i = 0;
        for (ScoreDoc d : t.scoreDocs) {
            suggestions[i++] = this.searcher.doc(d.doc).get(SHINGLE_FIELDNAME);
        }
        return suggestions;
    }

    public List<String> shingles(String s) throws IOException {
        List<String> shingles = new ArrayList<String>();
        TokenStream stream = this.shingleAnalyzer.tokenStream("ignored", s);
        stream.reset();
        CharTermAttribute termAttribute = stream.getAttribute(CharTermAttribute.class);
        while (stream.incrementToken()) {
            shingles.add(termAttribute.toString());
        }
        stream.close();
        return shingles;
    }

    public List<String> ngrams(String s, Boolean trigram) throws IOException {
        List<String> ngram = new ArrayList<String>();
        Analyzer ngramAnalyzer = trigram ? this.trigram : this.bigram;
        TokenStream stream = ngramAnalyzer.tokenStream("ignored", s);
        stream.reset();
        CharTermAttribute termAttribute = stream.getAttribute(CharTermAttribute.class);
        while (stream.incrementToken()) {
            ngram.add(termAttribute.toString());
        }
        stream.close();
        return ngram;
    }

    public int numDocs() {
        return this.reader.numDocs();
    }

    private static class ShingleAnalyzer extends Analyzer {
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


    private static class NGramAnalyzer extends Analyzer {
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
            NGramTokenFilter filter = new NGramTokenFilter(src, this.minShingleSize, this.maxShingleSize);
            return new TokenStreamComponents(source, filter);
        }
    }
}
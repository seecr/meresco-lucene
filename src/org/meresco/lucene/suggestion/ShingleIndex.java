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
import org.apache.lucene.analysis.ngram.NGramTokenFilter;
import org.apache.lucene.analysis.shingle.ShingleFilter;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.util.CharacterUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.io.Reader;
import java.io.IOException;
import java.io.File;

import org.meresco.lucene.search.TermFrequencySimilarity;

public class ShingleIndex {

    private IndexWriter writer;
    private DirectoryReader reader;
    private IndexSearcher searcher;
    private ShingleAnalyzer findShingles;
    private ShingleAnalyzer shingleAnalyzer;
    private NGramAnalyzer bigram;
    private NGramAnalyzer trigram;
    private FSDirectory directory;

    private static final String RECORD_ID_FIELD = "__id__";
    private static final String RECORD_SHINGLE_FIELD = "__record_shingle__";
    private static final String SHINGLE_FIELD = "__shingle__";
    private static final String BIGRAM_FIELD = "__bigram__";
    private static final String TRIGRAM_FIELD = "__trigram__";
    private static final String FREQUENCY_FIELD = "__freq__";

    private final int maxCommitTimeout;
    private final int maxCommitCount;
    private int commitCount = 0;
    private long lastUpdate = -1;

    private Map<String, Integer> lastDocFreqs = new HashMap<String, Integer>();

    public ShingleIndex(String directory, int minShingleSize, int maxShingleSize) throws IOException {
        this(directory, minShingleSize, maxShingleSize, 1, 0);
    }

    public ShingleIndex(String directory, int minShingleSize, int maxShingleSize, int commitCount, int commitTimeout) throws IOException {
        this.maxCommitCount = commitCount;
        this.maxCommitTimeout = commitTimeout;

        this.shingleAnalyzer = new ShingleAnalyzer(minShingleSize, maxShingleSize);
        this.bigram = new NGramAnalyzer(2, 2);
        this.trigram = new NGramAnalyzer(3, 3);

        this.directory = FSDirectory.open(new File(directory));
        IndexWriterConfig config = new IndexWriterConfig(Version.LATEST, new StandardAnalyzer());
        this.writer = new IndexWriter(this.directory, config);
        this.writer.commit();

        this.reader = DirectoryReader.open(this.directory);
        this.searcher = new IndexSearcher(this.reader);
        this.searcher.setSimilarity(new TermFrequencySimilarity());
    }

    public void add(String identifier, String[] values) throws IOException {
        Document recordDoc = new Document();
        recordDoc.add(new StringField(RECORD_ID_FIELD, identifier, Field.Store.NO));
        for (String value : values) {
            for (String shingle : shingles(value)) {
                recordDoc.add(new StringField(RECORD_SHINGLE_FIELD, shingle, Field.Store.NO));
                Document doc = new Document();
                doc.add(new StringField(SHINGLE_FIELD, shingle, Field.Store.YES));
                for (String n : ngrams(shingle, false)) {
                    doc.add(new StringField(BIGRAM_FIELD, n, Field.Store.NO));
                }
                for (String n : ngrams(shingle, true)) {
                    doc.add(new StringField(TRIGRAM_FIELD, n, Field.Store.NO));
                }
                doc.add(new TextField(FREQUENCY_FIELD, xForDocFreq(shingle), Field.Store.NO));
                this.writer.updateDocument(new Term(SHINGLE_FIELD, shingle), doc);
            }
        }
        this.writer.updateDocument(new Term(RECORD_ID_FIELD, identifier), recordDoc);

        maybeCommitAfterUpdate();
    }

    private String xForDocFreq(String shingle) throws IOException{
        maybeCommitForQuery();
        Integer docFreq = this.lastDocFreqs.get(shingle);
        if (docFreq == null) {
            docFreq = this.reader.docFreq(new Term(RECORD_SHINGLE_FIELD, shingle));
        }
        docFreq++;
        this.lastDocFreqs.put(shingle, docFreq);

        char[] buffer = new char[docFreq*2];
        for(int i = 0; i < docFreq; i++){
            buffer[2*i] = 'x';
            buffer[2*i+1] = ' ';
        }
        return new String(buffer);
    }

    public String[] suggest(String value, Boolean trigram) throws IOException {
        maybeCommitForQuery();
        String ngramFieldName = trigram ? TRIGRAM_FIELD : BIGRAM_FIELD;
        BooleanQuery query = new BooleanQuery();
        List<String> ngrams = ngrams(value, trigram);
        int SKIP_LAST_DOLLAR = 1;
        int ngramSize = ngrams.size() - SKIP_LAST_DOLLAR;
        for (int i = 0; i < ngramSize; i++) {
            query.add(new TermQuery(new Term(ngramFieldName, ngrams.get(i))), BooleanClause.Occur.MUST);
        }
        if (ngramSize > 0) {
            query.add(new TermQuery(new Term(FREQUENCY_FIELD, "x")), BooleanClause.Occur.MUST);
        }
        TopDocs t = this.searcher.search(query, 25);
        String[] suggestions = new String[t.totalHits < 25 ? t.totalHits : 25];
        int i = 0;
        for (ScoreDoc d : t.scoreDocs) {
            suggestions[i++] = this.searcher.doc(d.doc).get(SHINGLE_FIELD);
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

    private void maybeCommitAfterUpdate() throws IOException {
        this.commitCount++;
        this.lastUpdate = System.currentTimeMillis();
        if (this.commitCount >= this.maxCommitCount) {
            this.commit();
        }
    }

    private void maybeCommitForQuery() throws IOException {
        if (this.lastUpdate == -1) {
            return;
        }
        if (System.currentTimeMillis() - this.lastUpdate >= this.maxCommitTimeout) {
            this.commit();
        }
    }

    public void commit() throws IOException {
        this.writer.commit();
        this.lastUpdate  = -1;
        this.commitCount = 0;
        this.lastDocFreqs = new HashMap<String, Integer>();
        DirectoryReader newReader = DirectoryReader.openIfChanged(this.reader);
        if (newReader != null) {
            this.reader = newReader;
            this.searcher = new IndexSearcher(this.reader);
            this.searcher.setSimilarity(new TermFrequencySimilarity());
        }
    }

    public void close() throws IOException {
        this.reader.close();
        this.writer.close();
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
            src = new AddWordBoundaryFilter(src);
            NGramTokenFilter filter = new NGramTokenFilter(src, this.minShingleSize, this.maxShingleSize);
            return new TokenStreamComponents(source, filter);
        }
    }

    private static class AddWordBoundaryFilter extends TokenFilter {
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
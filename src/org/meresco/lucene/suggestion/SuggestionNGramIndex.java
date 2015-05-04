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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.lucene.search.similarities.TFIDFSimilarity;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Version;
import org.meresco.lucene.search.TermFrequencySimilarity;
import org.meresco.lucene.suggestion.SuggestionIndex.IndexingState;
import java.util.concurrent.Executors;

public class SuggestionNGramIndex {

	private static final String SUGGESTION_FIELDNAME = "__suggestion__";
	private static final String CONCEPT_URI_FIELDNAME = "__uri__";
    private static final String BIGRAM_FIELDNAME = "__bigram__";
    private static final String TRIGRAM_FIELDNAME = "__trigram__";

    public static final FieldType RANK_FIELD_TYPE = new FieldType();
    static {
        RANK_FIELD_TYPE.setIndexed(true);
        RANK_FIELD_TYPE.setIndexOptions(IndexOptions.DOCS_AND_FREQS);
        RANK_FIELD_TYPE.setTokenized(true);
        RANK_FIELD_TYPE.setStored(false);
        RANK_FIELD_TYPE.setOmitNorms(true);
        RANK_FIELD_TYPE.freeze();
    }

    private Field suggestionField = new Field(SUGGESTION_FIELDNAME, "", SuggestionIndex.SIMPLE_STORED_STRING_FIELD);
    private Field conceptUriField = new Field(CONCEPT_URI_FIELDNAME, "", SuggestionIndex.SIMPLE_STORED_STRING_FIELD);

    private final NGramAnalyzer bigram;
    private final NGramAnalyzer trigram;
    private final int maxCommitCount;

    private final IndexWriter writer;
    private final FSDirectory directory;
	private int commitCount;
	public long indexingTermsCount;
	public long totalTerms;

	public SuggestionNGramIndex(String directory) throws IOException {
        this(directory, 1);
    }

	public SuggestionNGramIndex(String directory, int commitCount) throws IOException {
		this.maxCommitCount = commitCount;

        this.bigram = new NGramAnalyzer(2, 2);
        this.trigram = new NGramAnalyzer(3, 3);

        this.directory = FSDirectory.open(new File(directory));
        IndexWriterConfig config = new IndexWriterConfig(Version.LATEST, new StandardAnalyzer());
        this.writer = new IndexWriter(this.directory, config);
        this.writer.commit();
	}

	public void createSuggestions(IndexReader reader, String suggestionFieldname, IndexingState indexingState) throws IOException {
        Terms terms = MultiFields.getTerms(reader, suggestionFieldname);
        if (terms == null)
            return;
		TermsEnum iterator = terms.iterator(null);
    	BytesRef term;
    	while ((term = iterator.next()) != null) {
            String[] values = term.utf8ToString().split("\\|");
            indexNGram(values[0], values[1]);
            indexingState.count++;
    	}
    	this.commit();
    }

	private void maybeCommitAfterUpdate() throws IOException {
        this.commitCount++;
        if (this.commitCount >= this.maxCommitCount) {
            this.commit();
        }
    }

    public void commit() throws IOException {
        this.writer.commit();
        this.commitCount = 0;
    }

    public void close() throws IOException {
        this.writer.close();
    }

    private void indexNGram(String type, String term) throws IOException {
        Document doc = new Document();
        this.suggestionField.setStringValue(term);
        doc.add(this.suggestionField);
        if (!type.equals("")) {
            this.conceptUriField.setStringValue(type);
            doc.add(this.conceptUriField);
        }
        for (String n : ngrams(term, false)) {
            doc.add(new Field(BIGRAM_FIELDNAME, n, SuggestionIndex.SIMPLE_NOT_STORED_STRING_FIELD));
        }
        for (String n : ngrams(term, true)) {
            doc.add(new Field(TRIGRAM_FIELDNAME, n, SuggestionIndex.SIMPLE_NOT_STORED_STRING_FIELD));
        }
        this.writer.addDocument(doc);
        maybeCommitAfterUpdate();
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

    public Reader getReader() throws IOException {
    	return new Reader();
    }

    public class Reader {
    	private DirectoryReader reader;
        private IndexSearcher searcher;

    	public Reader() throws IOException {
    		this.reader = DirectoryReader.open(directory);
            this.searcher = new IndexSearcher(this.reader, Executors.newFixedThreadPool(10));
    	}

    	public void maybeReopen() throws IOException {
    		DirectoryReader newReader = DirectoryReader.openIfChanged(this.reader);
            if (newReader != null) {
                this.reader = newReader;
                this.searcher = new IndexSearcher(this.reader);
            }
    	}

        public int numDocs() throws IOException {
        	maybeReopen();
            return this.reader.numDocs();
        }

    	public Suggestion[] suggest(String value, Boolean trigram) throws IOException {
            maybeReopen();
            String ngramFieldName = trigram ? TRIGRAM_FIELDNAME : BIGRAM_FIELDNAME;
            BooleanQuery query = new BooleanQuery();
            List<String> ngrams = ngrams(value, trigram);
            int SKIP_LAST_DOLLAR = 1;
            int ngramSize = ngrams.size() - SKIP_LAST_DOLLAR;
            for (int i = 0; i < ngramSize; i++) {
                query.add(new TermQuery(new Term(ngramFieldName, ngrams.get(i))), BooleanClause.Occur.MUST);
            }
            TopDocs t = searcher.search(query, 25);
            Suggestion[] suggestions = new Suggestion[t.totalHits < 25 ? t.totalHits : 25];
            int i = 0;
            for (ScoreDoc d : t.scoreDocs) {
                Document doc = searcher.doc(d.doc);
                suggestions[i++] = new Suggestion(doc.get(SUGGESTION_FIELDNAME), doc.get(CONCEPT_URI_FIELDNAME), d.score);
            }
            return suggestions;
        }

        public void close() throws IOException {
            this.reader.close();
        }
    }

    public class Suggestion {
    	public String suggestion;
    	public String type;
    	public float score;

    	public Suggestion(String suggestion, String type, float score) {
    		this.suggestion = suggestion;
    		this.type = type;
    		this.score = score;
    	}
    }
}

/* begin license *
 *
 * "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
 *
 * Copyright (C) 2015-2016 Koninklijke Bibliotheek (KB) http://www.kb.nl
 * Copyright (C) 2015-2016 Seecr (Seek You Too B.V.) http://seecr.nl
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
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.meresco.lucene.Utils;
import org.meresco.lucene.suggestion.SuggestionIndex.IndexingState;


public class SuggestionNGramIndex {
	private static final String SUGGESTION_FIELDNAME = "__suggestion__";
    private static final String CONCEPT_URI_FIELDNAME = "type";
    private static final String BIGRAM_FIELDNAME = "__bigram__";
    private static final String TRIGRAM_FIELDNAME = "__trigram__";
    private static final String CREATOR_FIELDNAME = "creator";
    private static final String KEY_FIELDNAME = "__keys__";

    private static final NGramAnalyzer BIGRAM_ANALYZER = new NGramAnalyzer(2, 2);
    private static final NGramAnalyzer TRIGRAM_ANALYZER = new NGramAnalyzer(3, 3);

    private Field suggestionField = new Field(SUGGESTION_FIELDNAME, "", SuggestionIndex.SIMPLE_STORED_STRING_FIELD);
    private Field conceptUriField = new Field(CONCEPT_URI_FIELDNAME, "", SuggestionIndex.SIMPLE_STORED_STRING_FIELD);
    private Field creatorField = new Field(CREATOR_FIELDNAME, "", SuggestionIndex.SIMPLE_STORED_STRING_FIELD);
    private Field keyField = new BinaryDocValuesField(KEY_FIELDNAME, new BytesRef());

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
        this.directory = FSDirectory.open(Paths.get(directory));
        IndexWriterConfig config = new IndexWriterConfig(new StandardAnalyzer());
        this.writer = new IndexWriter(this.directory, config);
        this.writer.commit();
	}

	public void createSuggestions(IndexReader reader, String suggestionFieldname, String keyFieldname, IndexingState indexingState) throws IOException {
        Bits liveDocs = MultiFields.getLiveDocs(reader);
        List<LeafReaderContext> leaves = reader.leaves();
        Terms terms = MultiFields.getTerms(reader, suggestionFieldname);
        if (terms == null)
            return;
		TermsEnum termsEnum = terms.iterator();
    	BytesRef term;
    	while ((term = termsEnum.next()) != null) {
    	    List<Long> keys = new ArrayList<>();
    	    PostingsEnum postings = termsEnum.postings(null, PostingsEnum.NONE);
    	    while (true) {
                int docId = postings.nextDoc();
                if (docId == PostingsEnum.NO_MORE_DOCS)
                    break;
                if (liveDocs != null && !liveDocs.get(docId))
                    continue;
                keys.add(keyForDoc(docId, leaves, keyFieldname));
    	    }
            if (keys.size() > 0) {
                String[] values = term.utf8ToString().split(SuggestionIndex.CONCAT_MARKER.replace("$", "\\$"));
                indexNGram(values[0], values[1], values[2], keys);
                indexingState.count++;
            }
    	}
    	this.commit();
    }

	private Long keyForDoc(int docId, List<LeafReaderContext> leaves, String keyFieldname) throws IOException {
	    LeafReaderContext context = leaves.get(ReaderUtil.subIndex(docId, leaves));
        NumericDocValues keys = context.reader().getNumericDocValues(keyFieldname);
        if (!keys.advanceExact(docId - context.docBase)) {
            return 0L;  // mimics behaviour of e.g. the old DocValues.emptyNumeric()
        }
        return keys.longValue();
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

    private void indexNGram(String type, String creator, String term, List<Long> keys) throws IOException {
        Document doc = new Document();
        this.suggestionField.setStringValue(term);
        doc.add(this.suggestionField);
        if (!type.equals("")) {
            this.conceptUriField.setStringValue(type);
            doc.add(this.conceptUriField);
        }
        if (!creator.equals("")) {
            this.creatorField.setStringValue(creator);
            doc.add(this.creatorField);
        }
        for (String n : ngrams(term, false)) {
            doc.add(new Field(BIGRAM_FIELDNAME, n, SuggestionIndex.SIMPLE_NOT_STORED_STRING_FIELD));
        }
        for (String n : ngrams(term, true)) {
            doc.add(new Field(TRIGRAM_FIELDNAME, n, SuggestionIndex.SIMPLE_NOT_STORED_STRING_FIELD));
        }
        keyField.setBytesValue(new BytesRef(Utils.join(keys, "|")));
        doc.add(keyField);
        this.writer.addDocument(doc);
        maybeCommitAfterUpdate();
    }

    public static List<String> ngrams(String s, Boolean trigram) throws IOException {
        List<String> ngram = new ArrayList<>();
        Analyzer ngramAnalyzer = trigram ? TRIGRAM_ANALYZER : BIGRAM_ANALYZER;
        TokenStream stream = ngramAnalyzer.tokenStream("ignored", s);
        stream.reset();
        CharTermAttribute termAttribute = stream.getAttribute(CharTermAttribute.class);
        while (stream.incrementToken()) {
            ngram.add(termAttribute.toString());
        }
        stream.close();
        return ngram;
    }

    public Reader createReader(Map<String, Bits> keySetFilters) throws IOException {
    	return new Reader(directory, keySetFilters);
    }


    public static class Reader {
        private FSDirectory directory;
    	private DirectoryReader reader;
        private IndexSearcher searcher;
        private Map<String, Bits> filterKeySets;
        private Map<String, Query> keySetFilters = new HashMap<>();

    	public Reader(FSDirectory directory, Map<String, Bits> filterKeySets) throws IOException {
    	    this.directory = directory;
            this.filterKeySets = filterKeySets;
    		reopen();
    	}

        public int numDocs() throws IOException {
            return this.reader.numDocs();
        }

        public Suggestion[] suggest(String value, Boolean trigram, String[] filters) throws Exception {
            return suggest(value, trigram, filters, null, 0);
        }

    	public Suggestion[] suggest(String value, Boolean trigram, String[] filters, String keySetName, int limit) throws Exception {
            if (limit == 0) {
                limit = 25;
            }
            String ngramFieldName = trigram ? TRIGRAM_FIELDNAME : BIGRAM_FIELDNAME;
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            List<String> ngrams = ngrams(value, trigram);
            int SKIP_LAST_DOLLAR = 1;
            int ngramSize = ngrams.size() - SKIP_LAST_DOLLAR;
            for (int i = 0; i < ngramSize; i++) {
                builder.add(new TermQuery(new Term(ngramFieldName, ngrams.get(i))), BooleanClause.Occur.MUST);
            }
            Query keySetFilter = this.keySetFilters.get(keySetName);
            if (keySetFilter == null) {
                Bits keys = filterKeySets.get(keySetName);
                if (keys != null) {
                    keySetFilter = new SuggestionNGramKeysFilter(keys, KEY_FIELDNAME);
                    this.keySetFilters.put(keySetName, keySetFilter);
                }
            }
            Query filter = createFilter(filters);
            if (filter != null) {
                builder.add(filter, Occur.FILTER);
            }
            if (keySetFilter != null) {
                builder.add(keySetFilter, Occur.FILTER);
            }
            TopDocs t = searcher.search(builder.build(), limit);
            Suggestion[] suggestions = new Suggestion[t.totalHits < limit ? (int) t.totalHits : limit];
            int i = 0;
            for (ScoreDoc d : t.scoreDocs) {
                Document doc = searcher.doc(d.doc);
                suggestions[i++] = new Suggestion(doc.get(SUGGESTION_FIELDNAME), doc.get(CONCEPT_URI_FIELDNAME), doc.get(CREATOR_FIELDNAME), d.score);
            }
            return suggestions;
        }

        public Query createFilter(String[] filters) {
            if (filters == null || filters.length == 0)
                return null;
            Query[] chain = new Query[filters.length];
            for (int i=0; i<filters.length; i++) {
                String filterString = filters[i];
                String[] f = filterString.split("=", 2);
                chain[i] = new TermQuery(new Term(f[0], f[1]));
            }
            if (chain.length == 1)
                return chain[0];
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            Stream.of(chain).forEach(f -> builder.add(f, Occur.SHOULD));
            return builder.build();
        }

        public void close() throws IOException {
            this.reader.close();
        }

        public synchronized void reopen() throws IOException {
            this.reader = DirectoryReader.open(directory);
            this.keySetFilters.clear();
            this.searcher = new IndexSearcher(this.reader, Executors.newFixedThreadPool(10));
        }
    }


    public static class Suggestion {
    	public String suggestion;
        public String type;
    	public String creator;
    	public float score;

    	public Suggestion(String suggestion, String type, String creator, float score) {
    		this.suggestion = suggestion;
    		this.type = type;
            this.creator = creator;
    		this.score = score;
    	}
    }
}

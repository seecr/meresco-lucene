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

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Bits;
import org.meresco.lucene.suggestion.SuggestionNGramIndex.Reader;


public class SuggestionIndex {
    private static final String ID_FIELD = "__id__";

    public static final String CONCAT_MARKER = "$$--$$";

    private static final String RECORD_VALUE_FIELDNAME = "__record_value__";
    private static final String KEY_FIELDNAME = "__key__";
    private static int MAX_COMMIT_COUNT_SUGGESTION = 1000000;

    public static final FieldType SIMPLE_NOT_STORED_STRING_FIELD = new FieldType();
    public static final FieldType SIMPLE_STORED_STRING_FIELD = new FieldType();
    static {
        SIMPLE_NOT_STORED_STRING_FIELD.setIndexOptions(IndexOptions.DOCS_AND_FREQS);
        SIMPLE_NOT_STORED_STRING_FIELD.setOmitNorms(false);
        SIMPLE_NOT_STORED_STRING_FIELD.setStored(false);
        SIMPLE_NOT_STORED_STRING_FIELD.setTokenized(false);
        SIMPLE_NOT_STORED_STRING_FIELD.freeze();

        SIMPLE_STORED_STRING_FIELD.setIndexOptions(IndexOptions.DOCS);
        SIMPLE_STORED_STRING_FIELD.setOmitNorms(true);
        SIMPLE_STORED_STRING_FIELD.setStored(true);
        SIMPLE_STORED_STRING_FIELD.setTokenized(false);
        SIMPLE_STORED_STRING_FIELD.freeze();
    }

    public IndexingState indexingState = null;

    private final IndexWriter writer;
    private final ShingleAnalyzer shingleAnalyzer;
    private final FSDirectory suggestionIndexDir;
    private final int maxCommitCount;

    private int commitCount = 0;

    private SuggestionNGramIndex suggestionNGramIndex;
    private String suggestionNGramIndexDir;
    private Reader currentReader;
    private Map<String, Bits> filterKeySets = new HashMap<>();


    public SuggestionIndex(String suggestionIndexDir, String suggestionNGramIndexDir, int minShingleSize, int maxShingleSize) throws IOException {
        this(suggestionIndexDir, suggestionNGramIndexDir, minShingleSize, maxShingleSize, 1);
    }

    public SuggestionIndex(String suggestionIndexDir, String suggestionNGramIndexDir, int minShingleSize, int maxShingleSize, int commitCount) throws IOException {
        this.maxCommitCount = commitCount;

        this.shingleAnalyzer = new ShingleAnalyzer(minShingleSize, maxShingleSize);

        this.suggestionIndexDir = FSDirectory.open(Paths.get(suggestionIndexDir));
        IndexWriterConfig config = new IndexWriterConfig(new StandardAnalyzer());
        this.writer = new IndexWriter(this.suggestionIndexDir, config);
        this.writer.commit();
        this.suggestionNGramIndexDir = suggestionNGramIndexDir;
        this.suggestionNGramIndex = new SuggestionNGramIndex(this.suggestionNGramIndexDir, MAX_COMMIT_COUNT_SUGGESTION);
    }

    public void add(String identifier, int key, String[] values, String[] types, String[] creators) throws IOException {
        Document recordDoc = new Document();
        recordDoc.add(new Field(ID_FIELD, identifier, SIMPLE_NOT_STORED_STRING_FIELD));
        recordDoc.add(new NumericDocValuesField(KEY_FIELDNAME, key));
        for (int i = 0; i < values.length; i++) {
            String value = (types[i] != null ? types[i] : "") + CONCAT_MARKER + (creators[i] != null ? creators[i] : "") + CONCAT_MARKER + values[i];
            recordDoc.add(new Field(RECORD_VALUE_FIELDNAME, value, SIMPLE_NOT_STORED_STRING_FIELD));
        }
        this.writer.updateDocument(new Term(ID_FIELD, identifier), recordDoc);
        maybeCommitAfterUpdate();
    }

    public void delete(String identifier) throws IOException {
        this.writer.deleteDocuments(new Term(ID_FIELD, identifier));
        maybeCommitAfterUpdate();
    }

    public void registerFilterKeySet(String name, Bits keySet) throws IOException {
        this.filterKeySets.put(name, keySet);
    }

    public synchronized void createSuggestionNGramIndex(boolean wait, final boolean verbose) throws IOException {
        this.commit();
        Thread create = new Thread(){
            @Override
            public void run() {
                indexingState = new IndexingState();
                try {
                    DirectoryReader reader = DirectoryReader.open(suggestionIndexDir);
                    String tempDir = suggestionNGramIndexDir+"~";
                    String tempTempDir = suggestionNGramIndexDir+"~~";
                    deleteIndexDirectory(tempDir);
                    deleteIndexDirectory(tempTempDir);
                    SuggestionNGramIndex newSuggestionNGramIndex = new SuggestionNGramIndex(tempDir, MAX_COMMIT_COUNT_SUGGESTION);
                    newSuggestionNGramIndex.createSuggestions(reader, RECORD_VALUE_FIELDNAME, KEY_FIELDNAME, indexingState);
                    newSuggestionNGramIndex.close();
                    reader.close();
                    suggestionNGramIndex.close();
                    new File(suggestionNGramIndexDir).renameTo(new File(tempTempDir));
                    new File(tempDir).renameTo(new File(suggestionNGramIndexDir));

                    suggestionNGramIndex = new SuggestionNGramIndex(suggestionNGramIndexDir, MAX_COMMIT_COUNT_SUGGESTION);
                    deleteIndexDirectory(tempTempDir);

                    if (currentReader != null)
                        currentReader.reopen();
                    else
                        createSuggestionsReader();
                } catch (IOException e) {
                    e.printStackTrace();
                } finally {
                    long totalTime = (System.currentTimeMillis() - indexingState.started);
                    float averageSpeed = totalTime > 0 ? 1000 * (float) indexingState.count / totalTime : 0;
                    if (verbose) {
                        System.out.println("Creating " + indexingState.count + " suggestions took: " + totalTime + "ms" + "; Average: " + averageSpeed + "/s");
                        System.out.flush();
                    }
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                    }
                    indexingState = null;
                }
            }

            private void deleteIndexDirectory(String dir) {
                File[] files = new File(dir).listFiles();
                if (files != null) {
                    for(File currentFile: new File(dir).listFiles()) {
                        currentFile.delete();
                    }
                    new File(dir).delete();
                }
            }
        };
        if (wait)
            create.run();
        else
            create.start();
    }

    public IndexingState indexingState() {
        return indexingState;
    }

    public int numDocs() throws IOException {
        DirectoryReader reader = DirectoryReader.open(this.suggestionIndexDir);
        int numDocs = reader.numDocs();
        reader.close();
        return numDocs;
    }

    public long ngramIndexTimestamp(){
        return new File(suggestionNGramIndexDir).lastModified();
    }

    public void createSuggestionsReader() throws IOException {
        this.currentReader = this.suggestionNGramIndex.createReader(this.filterKeySets);
    }

    public SuggestionNGramIndex.Reader getSuggestionsReader() throws IOException {
        if (this.currentReader == null)
            createSuggestionsReader();
        return this.currentReader;
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
        this.suggestionNGramIndex.close();
    }

    public List<String> shingles(String s) throws IOException {
        List<String> shingles = new ArrayList<>();
        TokenStream stream = this.shingleAnalyzer.tokenStream("ignored", s);
        stream.reset();
        CharTermAttribute termAttribute = stream.getAttribute(CharTermAttribute.class);
        while (stream.incrementToken()) {
            shingles.add(termAttribute.toString());
        }
        stream.close();
        return shingles;
    }

    public class IndexingState {
        public long started;
        public int count;

        public IndexingState() {
            started = System.currentTimeMillis();
            count = 0;
        }
    }
}

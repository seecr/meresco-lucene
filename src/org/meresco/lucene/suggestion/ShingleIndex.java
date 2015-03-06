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

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

public class ShingleIndex {

    private static final String RECORD_SHINGLE_FIELDNAME = "__record_shingle__";

    public static final FieldType SIMPLE_NOT_STORED_STRING_FIELD = new FieldType();
    public static final FieldType SIMPLE_STORED_STRING_FIELD = new FieldType();
    static {
        SIMPLE_NOT_STORED_STRING_FIELD.setIndexed(true);
        SIMPLE_NOT_STORED_STRING_FIELD.setIndexOptions(IndexOptions.DOCS_ONLY);
        SIMPLE_NOT_STORED_STRING_FIELD.setOmitNorms(true);
        SIMPLE_NOT_STORED_STRING_FIELD.setStored(false);
        SIMPLE_NOT_STORED_STRING_FIELD.setTokenized(false);
        SIMPLE_NOT_STORED_STRING_FIELD.freeze();

        SIMPLE_STORED_STRING_FIELD.setIndexed(true);
        SIMPLE_STORED_STRING_FIELD.setIndexOptions(IndexOptions.DOCS_ONLY);
        SIMPLE_STORED_STRING_FIELD.setOmitNorms(true);
        SIMPLE_STORED_STRING_FIELD.setStored(true);
        SIMPLE_STORED_STRING_FIELD.setTokenized(false);
        SIMPLE_STORED_STRING_FIELD.freeze();
    }

    private final IndexWriter writer;
    private final ShingleAnalyzer shingleAnalyzer;
    private final FSDirectory shingleIndexDir;
    private final int maxCommitTimeout;
    private final int maxCommitCount;

    private int commitCount = 0;
    private long lastUpdate = -1;

    private Field recordIdField = new Field("__id__", "", SIMPLE_NOT_STORED_STRING_FIELD);

	private SuggestionIndex suggestionIndex;

    public ShingleIndex(String shingleIndexDir, String suggestionIndexDir, int minShingleSize, int maxShingleSize) throws IOException {
        this(shingleIndexDir, suggestionIndexDir, minShingleSize, maxShingleSize, 1, 0);
    }

    public ShingleIndex(String shingleIndexDir, String suggestionIndexDir, int minShingleSize, int maxShingleSize, int commitCount, int commitTimeout) throws IOException {
        this.maxCommitCount = commitCount;
        this.maxCommitTimeout = commitTimeout;

        this.shingleAnalyzer = new ShingleAnalyzer(minShingleSize, maxShingleSize);

        this.shingleIndexDir = FSDirectory.open(new File(shingleIndexDir));
        IndexWriterConfig config = new IndexWriterConfig(Version.LATEST, new StandardAnalyzer());
        this.writer = new IndexWriter(this.shingleIndexDir, config);
        this.writer.commit();

        this.suggestionIndex = new SuggestionIndex(suggestionIndexDir, this.maxCommitCount, this.maxCommitTimeout);
    }

    public void add(String identifier, String[] values) throws IOException {
        Document recordDoc = new Document();
        this.recordIdField.setStringValue(identifier);
        recordDoc.add(this.recordIdField);
        for (String value : values) {
            for (String shingle : shingles(value)) {
                recordDoc.add(new Field(RECORD_SHINGLE_FIELDNAME, shingle, SIMPLE_NOT_STORED_STRING_FIELD));
            }
        }
        this.writer.updateDocument(new Term(this.recordIdField.name(), identifier), recordDoc);
        maybeCommitAfterUpdate();
    }

    public void delete(String identifier) throws IOException {
        this.writer.deleteDocuments(new Term(this.recordIdField.name(), identifier));
        maybeCommitAfterUpdate();
    }

    public void createSuggestionIndex() throws IOException {
    	this.commit();
    	DirectoryReader reader = DirectoryReader.open(this.shingleIndexDir);
    	this.suggestionIndex.createSuggestions(reader, RECORD_SHINGLE_FIELDNAME);
        this.suggestionIndex.commit();
        reader.close();
    }

    public int numDocs() throws IOException {
        DirectoryReader reader = DirectoryReader.open(this.shingleIndexDir);
        int numDocs = reader.numDocs();
        reader.close();
        return numDocs;
    }

    public SuggestionIndex.Reader getSuggestionsReader() throws IOException {
        return this.suggestionIndex.getReader();
    }

    private void maybeCommitAfterUpdate() throws IOException {
        this.commitCount++;
        this.lastUpdate = System.currentTimeMillis();
        if (this.commitCount >= this.maxCommitCount) {
            this.commit();
        }
    }

    public void commit() throws IOException {
        this.writer.commit();
        this.lastUpdate  = -1;
        this.commitCount = 0;
    }

    public void close() throws IOException {
        this.writer.close();
        this.suggestionIndex.close();
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

}
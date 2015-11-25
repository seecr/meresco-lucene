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

package org.meresco.lucene;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.store.Directory;
import org.meresco.lucene.search.SuperIndexSearcher;

public class IndexAndTaxanomy {
    private int numberOfConcurrentTasks;
    DirectoryReader reader;
    DirectoryTaxonomyReader taxoReader;
    private ExecutorService executor = null;
    private SuperIndexSearcher searcher;
    private boolean reopenSearcher = true;
    private LuceneSettings settings;

    public IndexAndTaxanomy(Directory indexDirectory, Directory taxoDirectory, LuceneSettings settings) throws IOException {
        this.numberOfConcurrentTasks = settings.numberOfConcurrentTasks;
        this.reader = DirectoryReader.open(indexDirectory);
        this.taxoReader = new DirectoryTaxonomyReader(taxoDirectory);
        this.settings = settings;
    }

    public boolean reopen() throws IOException {
        DirectoryReader reader = DirectoryReader.openIfChanged(this.reader);
        if (reader == null)
            return false;
        this.reader.close();
        this.reader = reader;
        this.reopenSearcher = true;
        DirectoryTaxonomyReader taxoReader = DirectoryTaxonomyReader.openIfChanged(this.taxoReader);
        if (taxoReader == null)
            return true;
        this.taxoReader.close();
        this.taxoReader = taxoReader;
        return true;
    }

    public SuperIndexSearcher searcher() {
        if (!this.reopenSearcher)
            return this.searcher;

        if (this.executor != null)
            this.executor.shutdown();
        this.executor  = Executors.newFixedThreadPool(this.numberOfConcurrentTasks);
        this.searcher = new SuperIndexSearcher(this.reader, this.executor, this.numberOfConcurrentTasks);
        this.searcher.setSimilarity(this.settings.similarity);
        return this.searcher;
    }

    public void close() throws IOException {
        this.taxoReader.close();
        this.reader.close();
    }
}

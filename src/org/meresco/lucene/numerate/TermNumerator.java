/* begin license *
 *
 * "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
 *
 * Copyright (C) 2016 Seecr (Seek You Too B.V.) http://seecr.nl
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

package org.meresco.lucene.numerate;

import java.io.File;
import java.io.IOException;

import org.apache.lucene.facet.taxonomy.FacetLabel;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.facet.taxonomy.writercache.LruTaxonomyWriterCache;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.MMapDirectory;

public class TermNumerator {
    private DirectoryTaxonomyWriter taxoWriter;
    private DirectoryTaxonomyReader taxoReader;

    protected TermNumerator() {};  // for easier mocking by subclass
    
    public TermNumerator(File path) throws IOException {
        MMapDirectory taxoDirectory = new MMapDirectory(path);
        taxoDirectory.setUseUnmap(false);
        taxoWriter = new DirectoryTaxonomyWriter(taxoDirectory, IndexWriterConfig.OpenMode.CREATE_OR_APPEND, new LruTaxonomyWriterCache(100));
    }

    public int numerateTerm(String term) throws IOException {
        return taxoWriter.addCategory(new FacetLabel(term));
    }

    synchronized String getTerm(int nr) throws IOException {
        if (taxoReader == null) {
            taxoReader = new DirectoryTaxonomyReader(taxoWriter);
        }
        DirectoryTaxonomyReader tr = DirectoryTaxonomyReader.openIfChanged(taxoReader);
        if (tr != null) {
            taxoReader.close();
            taxoReader = tr;
        }
        return taxoReader.getPath(nr).components[0];
    }

    public synchronized void commit() throws IOException {
        taxoWriter.commit();
    }

    public void close() throws IOException {
        taxoWriter.close();
    }
}

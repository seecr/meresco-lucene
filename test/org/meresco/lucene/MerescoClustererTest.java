/* begin license *
 *
 * "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
 *
 * Copyright (C) 2015 Koninklijke Bibliotheek (KB) http://www.kb.nl
 * Copyright (C) 2015-2016 Seecr (Seek You Too B.V.) http://seecr.nl
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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;

import java.io.IOException;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexReader;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.meresco.lucene.search.MerescoCluster;
import org.meresco.lucene.search.MerescoClusterer;


public class MerescoClustererTest extends SeecrTestCase {
    private Lucene lucene;
    private MerescoClusterer merescoClusterer;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        Document doc;

        LuceneSettings settings = new LuceneSettings();
        settings.commitCount = 1;
        lucene = new Lucene(this.tmpDir, settings);
        FieldType fieldType = new FieldType(TextField.TYPE_NOT_STORED);
        fieldType.setStoreTermVectors(true);

        for (int i = 0; i < 5; i++) {
            doc = new Document();
            doc.add(new Field("termvector.field", "aap noot noot noot vuur", fieldType));
            lucene.addDocument("id:" + i, doc);
        }

        for (int i = 5; i < 10; i++) {
            doc = new Document();
            doc.add(new Field("termvector.field", "something else", fieldType));
            lucene.addDocument("id:" + i, doc);
        }
        for (int i = 10; i < 15; i++) {
            doc = new Document();
            doc.add(new Field("termvector.field", "iets anders", fieldType));
            lucene.addDocument("id:" + i, doc);
        }
        lucene.maybeCommitAfterUpdate();
        IndexReader reader = lucene.data.getManager().acquire().searcher.getIndexReader();
        merescoClusterer = new MerescoClusterer(reader, 0.5);
    }

    @After
    public void tearDown() throws Exception {
        lucene.close();
        super.tearDown();
    }

    @Test
    public void testClusterOnTermVectors() throws IOException {
        merescoClusterer.strategyClusterers.get(0).registerField("termvector.field", 1.0, null);
        for (int i = 0; i < 15; i++) {
            merescoClusterer.collect(i);
        }
        merescoClusterer.finish();

        MerescoCluster cluster1 = merescoClusterer.cluster(0);
        assertEquals(5, cluster1.topDocs.length);
        assertEquals(2, cluster1.topTerms.length);
        String[] terms = new String[cluster1.topTerms.length];
        for (int i = 0; i < terms.length; i++) {
            terms[i] = cluster1.topTerms[i].term;
        }
        assertArrayEquals(new String[] { "else", "something" }, terms);

        MerescoCluster cluster2 = merescoClusterer.cluster(5);
        assertEquals(5, cluster2.topDocs.length);
        terms = new String[cluster2.topTerms.length];
        for (int i = 0; i < terms.length; i++) {
            terms[i] = cluster2.topTerms[i].term;
        }
        assertArrayEquals(new String[] { "noot", "aap", "vuur" }, terms);

        MerescoCluster cluster3 = merescoClusterer.cluster(10);
        assertEquals(5, cluster3.topDocs.length);
        terms = new String[cluster3.topTerms.length];
        for (int i = 0; i < terms.length; i++) {
            terms[i] = cluster3.topTerms[i].term;
        }
        assertArrayEquals(new String[] { "anders", "iets" }, terms);

        assertNotSame(cluster1.topDocs, cluster2.topDocs);
        assertNotSame(cluster1.topDocs, cluster3.topDocs);
    }

    @Test
    public void testClusteringWithFieldFilter() throws IOException {
    	merescoClusterer.strategyClusterers.get(0).registerField("termvector.field", 1.0, "noot");
        for (int i = 0; i < 15; i++) {
            merescoClusterer.collect(i);
        }
        merescoClusterer.finish();

        MerescoCluster cluster1 = merescoClusterer.cluster(0);
        assertEquals(null, cluster1);

        MerescoCluster cluster2 = merescoClusterer.cluster(5);
        assertEquals(5, cluster2.topDocs.length);
        String[] terms = new String[cluster2.topTerms.length];
        for (int i = 0; i < terms.length; i++) {
            terms[i] = cluster2.topTerms[i].term;
        }
        assertArrayEquals(new String[] { "noot", "aap", "vuur" }, terms);

        MerescoCluster cluster3 = merescoClusterer.cluster(10);
        assertEquals(null, cluster3);
    }
}

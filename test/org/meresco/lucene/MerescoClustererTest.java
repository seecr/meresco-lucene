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

package org.meresco.lucene;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.math3.ml.clustering.Cluster;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexReader;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.meresco.lucene.Lucene.UninitializedException;
import org.meresco.lucene.search.InterpolateEpsilon;
import org.meresco.lucene.search.MerescoCluster;
import org.meresco.lucene.search.MerescoCluster.DocScore;
import org.meresco.lucene.search.MerescoCluster.TermScore;
import org.meresco.lucene.search.MerescoClusterer;
import org.meresco.lucene.search.MerescoVector;


public class MerescoClustererTest extends SeecrTestCase {
    private Lucene lucene;

    @Override
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
    }

    @Override
    @After
    public void tearDown() throws Exception {
        lucene.close();
        super.tearDown();
    }

    @SuppressWarnings("serial")
    @Test
    public void testClusterOnTermVectors() throws IOException, UninitializedException {
        ClusterConfig clusterConfig = new ClusterConfig().addStrategy(new ClusterStrategy(0.5, 1).addField("termvector.field", 1.0, null));
        MerescoClusterer merescoClusterer = new MerescoClusterer(getIndexReader(), clusterConfig);
        for (int i = 0; i < 15; i++) {
            merescoClusterer.collect(i);
        }
        merescoClusterer.finish();

        assertEquals(3, merescoClusterer.clusters.size());
        Object lastTopDocs = null;
        Set<List<String>> clusterTerms = new HashSet<List<String>>();
        for (Cluster<MerescoVector> cl : merescoClusterer.clusters) {
            int docId = cl.getPoints().get(0).docId();
            MerescoCluster cluster = merescoClusterer.cluster(docId);
            assertEquals(5, cluster.topDocs.length);
            assertNotSame(lastTopDocs, cluster.topDocs);
            lastTopDocs = cluster.topDocs;
            List<String> terms = new ArrayList<>();
            for (TermScore term : cluster.topTerms) {
                terms.add(term.term);
            }
            clusterTerms.add(terms);
        }
        assertEquals(new HashSet<List<String>>() {{
                this.add(Arrays.asList("else", "something"));
                this.add(Arrays.asList("noot", "aap", "vuur"));
                this.add(Arrays.asList("anders", "iets"));
        }}, clusterTerms);
    }

    @Test
    public void testClusteringWithFieldFilter() throws IOException, UninitializedException {
        ClusterConfig clusterConfig = new ClusterConfig().addStrategy(new ClusterStrategy(0.5, 1).addField("termvector.field", 1.0, "noot"));
        MerescoClusterer merescoClusterer = new MerescoClusterer(getIndexReader(), clusterConfig);
        for (int i = 0; i < 15; i++) {
            merescoClusterer.collect(i);
        }
        merescoClusterer.finish();

        assertEquals(1, merescoClusterer.clusters.size());
        Cluster<MerescoVector> theOneCluster = merescoClusterer.clusters.get(0);
        int docId = theOneCluster.getPoints().get(0).docId();
        MerescoCluster cluster = merescoClusterer.cluster(docId);
        assertEquals(5, cluster.topDocs.length);
        String[] terms = new String[cluster.topTerms.length];
        for (int i = 0; i < terms.length; i++) {
            terms[i] = cluster.topTerms[i].term;
        }
        assertArrayEquals(new String[] { "noot", "aap", "vuur" }, terms);
    }

    @Test
    public void testClusteringOnVectorsMultipleStrategies() throws IOException, Exception {
        ClusterConfig clusterConfig = new ClusterConfig(42);
        clusterConfig.addStrategy(new ClusterStrategy(0.5, 2).addField("termvector.field", 1.0, "vuur"));
        clusterConfig.addStrategy(new ClusterStrategy(0.4, 1).addField("termvector.field", 1.0, null));
        clusterConfig.addStrategy(new ClusterStrategy(0.4, 2).addField("termvector.field", 1.0, "anders"));

        InterpolateEpsilon interpolateEpsilon = new InterpolateEpsilon() {
            @Override
            public double interpolateEpsilon(long hits, int sliceSize, double clusteringEps, int clusterMoreRecords) {
                assertEquals(100, hits);
                assertEquals(10, sliceSize);
                assertTrue(clusteringEps >= 0.4);
                assertEquals(42, clusterMoreRecords);
                return clusteringEps;
            }
        };
        IndexReader indexReader = getIndexReader();
        MerescoClusterer merescoClusterer = new MerescoClusterer(indexReader, clusterConfig, interpolateEpsilon, 100, 10);
        for (int i = 0; i < 15; i++) {
            merescoClusterer.collect(i);
        }
        merescoClusterer.finish();

        assertEquals(3, merescoClusterer.clusters.size());
        for (int i = 0; i < 15; i++) {
            String theID = indexReader.document(i).get(Lucene.ID_FIELD);
            MerescoCluster cluster = merescoClusterer.cluster(i);
            Set<String> ids = new HashSet<>();
            for (DocScore ds : cluster.topDocs) {
                ids.add(indexReader.document(ds.docId).get(Lucene.ID_FIELD));
            }
            assertTrue(ids.contains(theID));
            int idOrd = Integer.valueOf(theID.split(":")[1]);
            if (0 <= idOrd && idOrd <= 4) {
                assertEquals(new HashSet<>(Arrays.asList("id:4", "id:0", "id:1", "id:2", "id:3")), ids);
            }
            else if (5 <= idOrd && idOrd <= 9) {
                assertEquals(new HashSet<>(Arrays.asList("id:8", "id:7", "id:6", "id:5", "id:9")), ids);
            }
            else {
                assertEquals(new HashSet<>(Arrays.asList("id:10", "id:11", "id:12", "id:13", "id:14")), ids);
            }
        }
    }


    private IndexReader getIndexReader() throws IOException, UninitializedException {
        return lucene.data.getManager().acquire().searcher.getIndexReader();
    }
}

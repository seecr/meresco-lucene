package org.meresco.lucene;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.Test;
import org.meresco.lucene.search.PageRank;
import org.meresco.lucene.search.PageRank.Node;

public class PageRankTest {

    @Test
    public void testCreate() {
        PageRank pr = new PageRank(5);
        pr.add(5, new double[] {0.3, 0.0, 0.4});  // docid, termvector
        pr.add(3, new double[] {0.2, 0.4});
        int nodes = pr.node_count;  // graph contains 2 docs and 3 terms
        assertEquals(5, nodes);
        pr.add(6, new double[] {0.3, 0.4, 0.4, 0.0, 0.1});
        nodes = pr.node_count;  // graph contains 3 docs and 4 terms
        assertEquals(7, nodes);
        pr.add(2, new double[] {0.0, 0.0, 0.0, 0.0, 0.0});
        nodes = pr.node_count;  // graph contains 3 docs and 4 terms
        assertEquals(8, nodes);
        try {
            pr.add(9, new double[] {1.0, 2.0, 3.0, 4.0, 5.0});
        } catch (Exception e) {
            assertTrue(e.toString().contains("ArrayIndexOutOfBoundsException: 4"));
        }
    }

    @Test
    public void testDocRanks() {
        PageRank pr = new PageRank(5);
        pr.add(50, new double[] {0.3, 0.0, 0.4});  // docid, termvector
        pr.add(30, new double[] {0.2, 0.4});
        pr.add(60, new double[] {0.3, 0.4, 0.4, 0.0, 0.1});
        pr.add(20, new double[] {0.0, 0.0, 0.0, 0.0, 0.0});
        pr.add(10, new double[] {0.2, 1.0, 2.0, 0.4, 0.0});
        pr.prepare();
        assertEquals(10, pr.node_count);
        List<Node> topDocs = pr.topDocs();
        List<Node> topTerms = pr.topTerms();
        double P = 1.0 / 10;
        double[] topDocsPrs = new double[] {P, P, P, P, P};
        int[] topTermsId = new int[] {0, 1, 2, 3, 4};
        int[] topDocsId = new int[] {50, 30, 60, 20, 10};
        int[] topDocsEdges = new int[] {2, 2, 4, 0, 4};
        int[] topTermsEdges = new int[] {4, 3, 3, 1, 1};
        for (int i = 0; i < topDocs.size(); i++) {
            assertEquals(topDocsPrs[i], topDocs.get(i).getPR(), 0);
            assertEquals(topTermsId[i], topTerms.get(i).id);
            assertEquals(topDocsId[i], topDocs.get(i).id);
            assertEquals(topDocsEdges[i], topDocs.get(i).edges);
            assertEquals(topTermsEdges[i], topTerms.get(i).edges);
        }
        pr.iterate();
        topDocs = pr.topDocs();
        topTerms = pr.topTerms();
        
        topDocsPrs = new double[] {0.27325000000000005, 0.1875416666666667, 0.16770833333333335, 0.16558333333333336, 0.15000000000000002};
        double[] topTermsPrs = new double[] {0.21800000000000003, 0.19675000000000004, 0.181875, 0.15850000000000003, 0.152125};
        topTermsId = new int[] {2, 1, 0, 3, 4};
        topDocsId = new int[] {10, 60, 50, 30, 20};
        topDocsEdges = new int[] {4, 4, 2, 2, 0};
        topTermsEdges = new int[] {3, 3, 4, 1, 1};
        for (int i = 0; i < topDocs.size(); i++) {
            assertEquals(topDocsPrs[i], topDocs.get(i).getPR(), 0);
            assertEquals(topTermsPrs[i], topTerms.get(i).getPR(), 0);
            assertEquals(topTermsId[i], topTerms.get(i).id);
            assertEquals(topDocsId[i], topDocs.get(i).id);
            assertEquals(topDocsEdges[i], topDocs.get(i).edges);
            assertEquals(topTermsEdges[i], topTerms.get(i).edges);
        }
        pr.iterate();
        topDocs = pr.topDocs();
        topTerms = pr.topTerms();
        topDocsPrs = new double[] {0.3908988541666667, 0.22153015625000003, 0.1863011979166667, 0.18002802083333336, 0.15000000000000002};
        topTermsPrs = new double[] {0.31058270833333335, 0.25215583333333336, 0.20902630208333337, 0.17322625000000003, 0.15398526041666669};
        topTermsId = new int[] {2, 1, 0, 3, 4};
        topDocsEdges = new int[] {4, 4, 2, 2, 0};
        topTermsEdges = new int[] {3, 3, 4, 1, 1};
        for (int i = 0; i < topDocs.size(); i++) {
            assertEquals(topDocsPrs[i], topDocs.get(i).getPR(), 0);
            assertEquals(topTermsPrs[i], topTerms.get(i).getPR(), 0);
            assertEquals(topTermsId[i], topTerms.get(i).id);
            assertEquals(topDocsId[i], topDocs.get(i).id);
            assertEquals(topDocsEdges[i], topDocs.get(i).edges);
            assertEquals(topTermsEdges[i], topTerms.get(i).edges);
        }
    }
}

/* begin license *
 *
 * "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
 *
 * Copyright (C) 2015 Koninklijke Bibliotheek (KB) http://www.kb.nl
 * Copyright (C) 2015 Seecr (Seek You Too B.V.) https://seecr.nl
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

package org.meresco.lucene.search;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class PageRank {
    // https://nl.wikipedia.org/wiki/PageRank

    public static final class Node implements Comparable<Node> {
        public final int id;
        private static double damping = 0.85;
        public int edges;
        private double PR0 = 0.0;
        private double PR1 = 0.0;

        public Node(int id) {
            this.id = id;
        }

        public void countEdge() {
            this.edges++;
        }

        public void setPR(double d) {
            this.PR0 = d;
        }

        public double getPR() {
            return this.PR0;
        }

        public void commitPR() {
            this.PR0 = (1.0 - damping) + damping * this.PR1;
            this.PR1 = 0.0;
        }

        public void addPR(Node node, double weight) {
            this.PR1 += node.PR0 / node.edges * weight;
        }

        @Override
        public int compareTo(Node rhs) {
            return Double.compare(rhs.PR0, this.PR0);
        }
    }

    private static final class Edge {
        private final Node lsh;
        private final Node rhs;
        private final double weight;

        public Edge(Node lhs, Node rhs, double weight) {
            this.lsh = lhs;
            this.rhs = rhs;
            this.weight = weight;
            lhs.countEdge();
            rhs.countEdge();
        }

        public void propagatePR() {
            this.lsh.addPR(this.rhs, this.weight);
            this.rhs.addPR(this.lsh, this.weight);
        }
    }

    private Node[] termnodes;
    private List<Node> docnodes = new ArrayList<Node>(); // dense, a few 10's
    private List<Edge> edges = new ArrayList<Edge>(); // dense, a few 10's
    public int node_count = 0;

    public PageRank(int max_ord) {
        this.termnodes = new Node[max_ord];
    }

    public void add(int docid, double[] docvector) {
        Node docnode = this.addDocNode(docid);
        for (int ord = 0; ord < docvector.length; ord++)
            if (docvector[ord] > 0.0)
                this.addEdge(docnode, this.addTermNode(ord), docvector[ord]);
    }

    private Node addDocNode(int docid) {
        Node docnode = this.createNode(docid);
        this.docnodes.add(docnode);
        return docnode;
    }

    private Node addTermNode(int ord) {
        if (this.termnodes[ord] == null)
            termnodes[ord] = this.createNode(ord);
        return this.termnodes[ord];
    }

    private Node createNode(int id) {
        this.node_count++;
        return new Node(id);
    }

    private void addEdge(Node lhs, Node rhs, double weight) {
        this.edges.add(new Edge(lhs, rhs, weight));
    }

    public void prepare() {
        double initial_rank = 1.0 / this.node_count;
        for (Node node : this.docnodes)
            node.setPR(initial_rank);
        for (Node node : this.termnodes)
            if (node != null)
                node.setPR(initial_rank);
    }

    public void iterate() {
        for (Edge edge : this.edges)
            edge.propagatePR();
        for (Node node : this.docnodes)
            node.commitPR();
        for (Node node : this.termnodes)
            if (node != null)
                node.commitPR();
    }

    public List<Node> topDocs() {
        Collections.sort(this.docnodes);
        return this.docnodes;
    }

    public List<Node> topTerms() {
        List<Node> result = new ArrayList<Node>();
        for (Node node : this.termnodes)
            if (node != null)
                result.add(node);
        Collections.sort(result);
        return result;
    }
}

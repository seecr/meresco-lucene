package org.meresco.lucene.search;

import java.util.ArrayList;
import java.util.List;

public class PageRank {
    // https://nl.wikipedia.org/wiki/PageRank

    public static class Node {
        public final int id;
        private static double damping = 0.85;
        private int edges;
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
    }

    static class Edge {
        private final Node docnode;
        private final Node termnode;
        private final double weight;

        public Edge(Node docnode, Node termnode, double weight) {
            this.docnode = docnode;
            this.termnode = termnode;
            this.weight = weight;
        }

        public void propagatePR() {
            this.docnode.addPR(this.termnode, this.weight);
            this.termnode.addPR(this.docnode, this.weight);
        }
    }

    private Node[] termnodes = new Node[100]; // sparse, a few 100's large TODO
    private List<Node> docnodes = new ArrayList<Node>(); // dense, a few 10's
    private List<Edge> edges = new ArrayList<Edge>(); // dense, a few 10's
    private int node_count = 0;

    public void add(int docid, double[] docvector) {
        Node docnode = this.addDocNode(docid);
        for (int ord = 0; ord < docvector.length; ord++) {
            Node termnode = this.addTermNode(ord);
            this.addEdge(docnode, termnode, docvector[ord]);
        }
    }

    private Node addDocNode(int docid) {
        Node docnode = new Node(docid);
        this.docnodes.add(docnode);
        docnode.countEdge();
        this.node_count++;
        return docnode;
    }

    private void addEdge(Node docnode, Node termnode, double weight) {
        this.edges.add(new Edge(docnode, termnode, weight));
    }

    private Node addTermNode(int ord) {
        Node termnode = this.termnodes[ord];
        if (termnode == null) {
            termnodes[ord] = termnode = new Node(ord);
            this.node_count++;
        }
        termnode.countEdge();
        return termnode;
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

    public double getDocRank(int i) {
        return this.docnodes.get(i).getPR();
    }
}

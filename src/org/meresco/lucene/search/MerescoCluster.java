package org.meresco.lucene.search;

public final class MerescoCluster {

    public final MerescoCluster.DocScore[] topDocs;
    public final MerescoCluster.TermScore[] topTerms;

    public MerescoCluster(MerescoCluster.DocScore[] topDocs, MerescoCluster.TermScore[] topTerms) {
        this.topDocs = topDocs;
        this.topTerms = topTerms;
    }

    public static final class DocScore {
        public final int docId;
        public final double score;
        public String identifier;
        
        public DocScore(int docId, double score) {
            this.docId = docId;
            this.score = score;
        }
    }

    public static final class TermScore {
        public final String term;
        public final double score;

        public TermScore(String term, double score) {
            this.term = term;
            this.score = score;
        }
    }

}
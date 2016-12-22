package org.meresco.lucene.search.join.relational;

import java.util.Map;

import org.meresco.lucene.Lucene;


public interface RelationalQuery {
    public KeyBits collectKeys(Map<String, Lucene> lucenes);

    Runner runner();

    interface Runner {
        public KeyBits collectKeys(Map<String, Lucene> lucenes);

        public void filter(KeyBits keyFilter);

        public void union(KeyBits intermediateResult);

        public void invert();
    }
}

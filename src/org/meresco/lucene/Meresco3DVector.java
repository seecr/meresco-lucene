package org.meresco.lucene;

import java.util.HashMap;
import java.util.Map;

import org.meresco.lucene.search.MerescoVector;

public class Meresco3DVector {

        private Map<String, MerescoVector> vectors = new HashMap<String, MerescoVector>();

        public void setEntry(String key, int i, double d) {
                this.vectors.get(key).setEntry(i, d);
        }

        public double distance(Meresco3DVector v2) {
                return 0.0;
        }

        public void setEntry(int index, double value) {
                this.vectors.get("default").setEntry(index, value);
        }

}

package org.meresco.lucene.http;

import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;

class QueryParameters extends HashMap<String, ArrayList<String>> {
    public String singleValue(String key) {
        List<String> l = get(key);
        if (l != null && l.size() == 1) {
            return l.get(0);
        }
        return null;
    }
}
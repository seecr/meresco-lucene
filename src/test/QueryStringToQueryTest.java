package test;

import static org.junit.Assert.*;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.junit.Before;
import org.junit.Test;
import org.meresco.lucene.QueryStringToQuery;

public class QueryStringToQueryTest {

    @Before
    public void setUp() throws Exception {
    }

    @Test
    public void testTermQuery() {
        assertEquals(new TermQuery(new Term("field", "value")), convert("{\"term\": {\"field\": \"field\", \"value\": \"value\"}, \"type\": \"TermQuery\"}"));
    }
    
    @Test
    public void testMatchAllDocsQuery() {
        assertEquals(new MatchAllDocsQuery(), convert("{\"type\": \"MatchAllDocsQuery\"}"));
    }

    private Query convert(String queryString) {
        return new QueryStringToQuery(queryString).convert();
    }
}

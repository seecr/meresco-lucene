package org.meresco.lucene.search.join;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.meresco.lucene.Lucene;
import org.meresco.lucene.LuceneResponse;
import org.meresco.lucene.LuceneSettings;
import org.meresco.lucene.LuceneTest;
import org.meresco.lucene.MultiLuceneTest;
import org.meresco.lucene.QueryData;
import org.meresco.lucene.SeecrTestCase;
import org.meresco.lucene.queries.KeyFilter;
import org.meresco.lucene.search.TermFrequencySimilarity;


public class RelationalQueryTest extends SeecrTestCase {
    private Lucene luceneA;
    private Lucene luceneB;
    private Lucene luceneC;

    @Override
	@Before
    public void setUp() throws Exception {
        super.setUp();
        LuceneSettings settingsA = new LuceneSettings();
        LuceneSettings settingsB = new LuceneSettings();
        LuceneSettings settingsC = new LuceneSettings();
        settingsC.similarity = new TermFrequencySimilarity();
        luceneA = new Lucene("coreA", this.tmpDir.resolve("a"), settingsA);
        luceneB = new Lucene("coreB", this.tmpDir.resolve("b"), settingsB);
        luceneC = new Lucene("coreC", this.tmpDir.resolve("c"), settingsC);
        MultiLuceneTest.prepareFixture(luceneA, luceneB, luceneC);
    }

    @Override
	@After
    public void tearDown() throws Exception {
        luceneA.close();
        luceneB.close();
        luceneC.close();
        super.tearDown();
    }

//    @Test
//    public void test() {
//        String query = "(summary.title =/boost=3 aap) AND (holding.holder = bieb)";
//
//        Query root = new JoinANDQuery(
//        new LuceneQuery("title= aap", "summary", "key", "ranks"),
//        new LuceneQuery("holder = bieb", "holding", "item", null));
//
//        root.execute(/* lucenesearcher */ null);
//    }

    @Test
    public void testSimpleJoinANDQuery() {
        RelationalQuery root = new JoinANDQuery(
        	new LuceneQuery(luceneB, "B", new TermQuery(new Term("N", "true"))  /* here all those args*/ ),
	        new LuceneQuery(luceneA, "A", new TermQuery(new Term("M", "true"))));
        IntermediateResult result = root.execute();
        assertEquals(4, result.getBitSet().cardinality());
        LuceneResponse response = responseForResult(result, luceneA, "A");
        LuceneTest.compareHits(response, "A-M", "A-MU", "A-MQ", "A-MQU");
    }

    @Test
    public void testJoinANDQueryResultFedToLuceneQuery() {
        RelationalQuery root = new JoinANDQuery(
            new JoinANDQuery(
                new LuceneQuery(luceneB, "B", new TermQuery(new Term("N", "true"))),
                new LuceneQuery(luceneA, "A", new TermQuery(new Term("M", "true")))),
            new LuceneQuery(luceneC, "C", new TermQuery(new Term("R", "true"))));
        IntermediateResult result = root.execute();
        LuceneResponse response = responseForResult(result, luceneA, "A");
        LuceneTest.compareHits(response, "A-M");
    }

    @Test
    public void testLuceneQueryResultFedToJoinANDQuery() {
        RelationalQuery root = new JoinANDQuery(
        	new LuceneQuery(luceneC, "C", new TermQuery(new Term("R", "true"))),
            new JoinANDQuery(
                new LuceneQuery(luceneB, "B", new TermQuery(new Term("N", "true"))),
                new LuceneQuery(luceneA, "A", new TermQuery(new Term("M", "true")))
            ));
        IntermediateResult result = root.execute();
        LuceneResponse response = responseForResult(result, luceneA, "A");
        LuceneTest.compareHits(response, "A-M");
    }

    @Test
    public void testMultipleJoinsWithSemanticallyIncompatibleKeys() {
        RelationalQuery root = new JoinANDQuery(
    		new JoinANDQuery(
    			new LuceneQuery(luceneC, "C2", new TermQuery(new Term("R", "true"))),
    			new LuceneQuery(luceneA, "A", "C", new TermQuery(new Term("M", "true")))
    		),
            new LuceneQuery(luceneB, "B", new TermQuery(new Term("N", "true")))
        );
        IntermediateResult result = root.execute();
        LuceneResponse response = responseForResult(result, luceneA, "A");
        LuceneTest.compareHits(response, "A-MU");
    }

    @Test
    public void testMultipleJoinsWithSemanticallyIncompatibleKeysToAndFro() {
        RelationalQuery root = new JoinANDQuery(
    		new JoinANDQuery(
	            new LuceneQuery(luceneB, "B", new TermQuery(new Term("N", "true"))),
	    		new JoinANDQuery(
	    			new LuceneQuery(luceneA, "C", "A", new TermQuery(new Term("M", "true"))),
					new LuceneQuery(luceneC, "C2", new TermQuery(new Term("R", "true")))
	    		)
	    	),
    		new LuceneQuery(luceneA, "A", "C", new MatchAllDocsQuery())
        );
        IntermediateResult result = root.execute();
        LuceneResponse response = responseForResult(result, luceneA, "A");
        LuceneTest.compareHits(response, "A-MU");
    }

    @Test
    public void testSimpleNotQuery() {
    	RelationalQuery root;
    	IntermediateResult result;
    	LuceneResponse response;

        root = new JoinANDQuery(
    		new LuceneQuery(luceneA, "A", new TermQuery(new Term("M", "true"))),
			new LuceneQuery(luceneB, "B", new TermQuery(new Term("O", "true")))  /* first without the NOT for reference only */
    	);
        result = root.execute();
        response = responseForResult(result, luceneA, "A");
        LuceneTest.compareHits(response, "A-M", "A-MQ");

        root = new JoinANDQuery(
    		new LuceneQuery(luceneA, "A", new TermQuery(new Term("M", "true"))),
    		new NotQuery(
    			new LuceneQuery(luceneB, "B", new TermQuery(new Term("O", "true")))
    		)
    	);
        result = root.execute();
        response = responseForResult(result, luceneA, "A");
        LuceneTest.compareHits(response, "A-MU", "A-MQU", "A-MQ");
    }

    @Test
    public void testNotQueryOverNotQuery() {
    	RelationalQuery root;
    	IntermediateResult result;
    	LuceneResponse response;

        root = new JoinANDQuery(
    		new LuceneQuery(luceneA, "A", new TermQuery(new Term("M", "true"))),
    		new NotQuery(
	    		new NotQuery(
	    			new LuceneQuery(luceneB, "B", new TermQuery(new Term("O", "true")))
	    		)
	    	)
    	);
        result = root.execute();
        response = responseForResult(result, luceneA, "A");
        LuceneTest.compareHits(response, "A-M", "A-MQ");
    }

    @Test
    public void testNotQueryOverJoinAND() {
    	RelationalQuery root;
    	IntermediateResult result;
    	LuceneResponse response;

        root = new JoinANDQuery(
    		new NotQuery(
	        	new JoinANDQuery(
	        		new LuceneQuery(luceneA, "A", new TermQuery(new Term("M", "true"))),
	        		new LuceneQuery(luceneB, "B", new TermQuery(new Term("O", "true")))
	        	)
	        ),
    		new LuceneQuery(luceneA, "A", new MatchAllDocsQuery())
    	);
        result = root.execute();
        response = responseForResult(result, luceneA, "A");
        LuceneTest.compareHits(response, "A", "A-U", "A-Q", "A-QU", "A-MU", "A-MQU");

        root = new NotQuery(
        	new JoinANDQuery(
        		new LuceneQuery(luceneA, "A", new TermQuery(new Term("M", "true"))),
        		new LuceneQuery(luceneB, "B", new TermQuery(new Term("O", "true")))
        	)
    	);
        result = root.execute();
        response = responseForResult(result, luceneA, "A");
        LuceneTest.compareHits(response, "A", "A-U", "A-Q", "A-QU", "A-MU", "A-MQU");
    }

    @Test
    public void testJoinANDOverNotQueryOverJoinAND() {
    	// requires explicit BitSet intersect... :-( !
    	RelationalQuery root;
    	IntermediateResult result;
    	LuceneResponse response;

        root = new JoinANDQuery(
    		new LuceneQuery(luceneA, "A", new TermQuery(new Term("Q", "true"))),
    		new NotQuery(
	        	new JoinANDQuery(
	        		new LuceneQuery(luceneB, "B", new TermQuery(new Term("O", "true"))),
	        		new LuceneQuery(luceneA, "A", new TermQuery(new Term("M", "true")))
	        	)
	        )
    	);
        result = root.execute();
        response = responseForResult(result, luceneA, "A");
        LuceneTest.compareHits(response, "A-Q", "A-QU", "A-MQU");
    }



    @Test
    public void testSimpleJoinORQuery() {
        RelationalQuery root = new JoinORQuery(
        	new LuceneQuery(luceneB, "B", new TermQuery(new Term("O", "true"))),
	        new LuceneQuery(luceneA, "A", new TermQuery(new Term("Q", "true"))));
        IntermediateResult result = root.execute();
        LuceneResponse response = responseForResult(result, luceneA, "A");
        LuceneTest.compareHits(response, "A-Q", "A-QU", "A-M", "A-MQ", "A-MQU");
    }


    private LuceneResponse responseForResult(IntermediateResult result, Lucene lucene, String keyName) {
        try {
        	Query keyFilter = new KeyFilter(result.getBitSet(), keyName);
        	if (result.inverted) {
                BooleanQuery.Builder builder = new BooleanQuery.Builder();
                builder.add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST);
                builder.add(keyFilter, BooleanClause.Occur.MUST_NOT);
                keyFilter = builder.build();
        	}
            return lucene.executeQuery(
                    new QueryData(),
                    null,
                    null,
                    Arrays.asList(keyFilter),
                    null,
                    null);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }
}

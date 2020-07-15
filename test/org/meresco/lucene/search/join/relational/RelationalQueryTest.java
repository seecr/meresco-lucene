/* begin license *
 *
 * "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
 *
 * Copyright (C) 2016 Seecr (Seek You Too B.V.) https://seecr.nl
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

package org.meresco.lucene.search.join.relational;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

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
    private Map<String, Lucene> lucenes;

    @SuppressWarnings("serial")
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
        this.lucenes = new HashMap<String, Lucene>() {{
            put("coreA", luceneA);
            put("coreB", luceneB);
            put("coreC", luceneC);
        }};
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

    @Test
    public void testSimpleJoinANDQuery() {
        RelationalQuery root = new JoinAndQuery(
            new RelationalLuceneQuery("coreB", "B", new TermQuery(new Term("N", "true"))  /* here all those args*/ ),
            new RelationalLuceneQuery("coreA", "A", new TermQuery(new Term("M", "true"))));
        KeyBits result = root.collectKeys(this.lucenes);
        assertEquals(4, result.bitset.cardinality());
        LuceneResponse response = responseForResult(result, luceneA, "A");
        LuceneTest.compareHits(response, "A-M", "A-MU", "A-MQ", "A-MQU");
    }

    @Test
    public void testJoinANDQueryResultFedToLuceneQuery() {
        RelationalQuery root = new JoinAndQuery(
            new JoinAndQuery(
                new RelationalLuceneQuery("coreB", "B", new TermQuery(new Term("N", "true"))),
                new RelationalLuceneQuery("coreA", "A", new TermQuery(new Term("M", "true")))),
            new RelationalLuceneQuery("coreC", "C", new TermQuery(new Term("R", "true"))));
        KeyBits result = root.collectKeys(this.lucenes);
        LuceneResponse response = responseForResult(result, luceneA, "A");
        LuceneTest.compareHits(response, "A-M");
    }

    @Test
    public void testLuceneQueryResultFedToJoinANDQuery() {
        RelationalQuery root = new JoinAndQuery(
            new RelationalLuceneQuery("coreC", "C", new TermQuery(new Term("R", "true"))),
            new JoinAndQuery(
                new RelationalLuceneQuery("coreB", "B", new TermQuery(new Term("N", "true"))),
                new RelationalLuceneQuery("coreA", "A", new TermQuery(new Term("M", "true")))
            ));
        KeyBits result = root.collectKeys(this.lucenes);
        LuceneResponse response = responseForResult(result, luceneA, "A");
        LuceneTest.compareHits(response, "A-M");
    }

    @Test
    public void testMultipleJoinsWithSemanticallyIncompatibleKeys() {
        RelationalQuery root = new JoinAndQuery(
            new JoinAndQuery(
                new RelationalLuceneQuery("coreC", "C2", new TermQuery(new Term("R", "true"))),
                new RelationalLuceneQuery("coreA", "A", "C", new TermQuery(new Term("M", "true")))
            ),
            new RelationalLuceneQuery("coreB", "B", new TermQuery(new Term("N", "true")))
        );
        KeyBits result = root.collectKeys(this.lucenes);
        LuceneResponse response = responseForResult(result, luceneA, "A");
        LuceneTest.compareHits(response, "A-MU");
    }

    @Test
    public void testMultipleJoinsWithSemanticallyIncompatibleKeysToAndFro() {
        RelationalQuery root = new JoinAndQuery(
            new JoinAndQuery(
                new RelationalLuceneQuery("coreB", "B", new TermQuery(new Term("N", "true"))),
                new JoinAndQuery(
                    new RelationalLuceneQuery("coreA", "C", "A", new TermQuery(new Term("M", "true"))),
                    new RelationalLuceneQuery("coreC", "C2", new TermQuery(new Term("R", "true")))
                )
            ),
            new RelationalLuceneQuery("coreA", "A", "C", new MatchAllDocsQuery())
        );
        KeyBits result = root.collectKeys(this.lucenes);
        LuceneResponse response = responseForResult(result, luceneA, "A");
        LuceneTest.compareHits(response, "A-MU");
    }

    @Test
    public void testSimpleNotQuery() {
        RelationalQuery root;
        KeyBits result;
        LuceneResponse response;

        root = new JoinAndQuery(
            new RelationalLuceneQuery("coreA", "A", new TermQuery(new Term("M", "true"))),
            new RelationalLuceneQuery("coreB", "B", new TermQuery(new Term("O", "true")))  /* first without the NOT for reference only */
        );
        result = root.collectKeys(this.lucenes);
        response = responseForResult(result, luceneA, "A");
        LuceneTest.compareHits(response, "A-M", "A-MQ");

        root = new JoinAndQuery(
            new RelationalLuceneQuery("coreA", "A", new TermQuery(new Term("M", "true"))),
            new RelationalNotQuery(
                new RelationalLuceneQuery("coreB", "B", new TermQuery(new Term("O", "true")))
            )
        );
        result = root.collectKeys(this.lucenes);
        response = responseForResult(result, luceneA, "A");
        LuceneTest.compareHits(response, "A-MU", "A-MQU", "A-MQ");
    }

    @Test
    public void testNotQueryOverNotQuery() {
        RelationalQuery root;
        KeyBits result;
        LuceneResponse response;

        root = new JoinAndQuery(
            new RelationalLuceneQuery("coreA", "A", new TermQuery(new Term("M", "true"))),
            new RelationalNotQuery(
                new RelationalNotQuery(
                    new RelationalLuceneQuery("coreB", "B", new TermQuery(new Term("O", "true")))
                )
            )
        );
        result = root.collectKeys(this.lucenes);
        response = responseForResult(result, luceneA, "A");
        LuceneTest.compareHits(response, "A-M", "A-MQ");
    }

    @Test
    public void testNotQueryOverJoinAND() {
        RelationalQuery root;
        KeyBits result;
        LuceneResponse response;

        root = new JoinAndQuery(
            new RelationalNotQuery(
                new JoinAndQuery(
                    new RelationalLuceneQuery("coreA", "A", new TermQuery(new Term("M", "true"))),
                    new RelationalLuceneQuery("coreB", "B", new TermQuery(new Term("O", "true")))
                )
            ),
            new RelationalLuceneQuery("coreA", "A", new MatchAllDocsQuery())
        );
        result = root.collectKeys(this.lucenes);
        response = responseForResult(result, luceneA, "A");
        LuceneTest.compareHits(response, "A", "A-U", "A-Q", "A-QU", "A-MU", "A-MQU");

        root = new RelationalNotQuery(
            new JoinAndQuery(
                new RelationalLuceneQuery("coreA", "A", new TermQuery(new Term("M", "true"))),
                new RelationalLuceneQuery("coreB", "B", new TermQuery(new Term("O", "true")))
            )
        );
        result = root.collectKeys(this.lucenes);
        response = responseForResult(result, luceneA, "A");
        LuceneTest.compareHits(response, "A", "A-U", "A-Q", "A-QU", "A-MU", "A-MQU");
    }

    @Test
    public void testJoinANDOverNotOverJoinANDQuery() {
        // requires explicit BitSet intersect... :-( !
        RelationalQuery root;
        KeyBits result;
        LuceneResponse response;

        root = new JoinAndQuery(
            new RelationalLuceneQuery("coreA", "A", new TermQuery(new Term("Q", "true"))),
            new RelationalNotQuery(
                new JoinAndQuery(
                    new RelationalLuceneQuery("coreB", "B", new TermQuery(new Term("O", "true"))),
                    new RelationalLuceneQuery("coreA", "A", new TermQuery(new Term("M", "true")))
                )
            )
        );
        result = root.collectKeys(this.lucenes);
        response = responseForResult(result, luceneA, "A");
        LuceneTest.compareHits(response, "A-Q", "A-QU", "A-MQU");
    }

    @Test
    public void testSimpleJoinORQuery() {
        RelationalQuery root = new JoinOrQuery(
            new RelationalLuceneQuery("coreB", "B", new TermQuery(new Term("O", "true"))),
            new RelationalLuceneQuery("coreA", "A", new TermQuery(new Term("Q", "true"))));
        KeyBits result = root.collectKeys(this.lucenes);
        LuceneResponse response = responseForResult(result, luceneA, "A");
        LuceneTest.compareHits(response, "A-Q", "A-QU", "A-M", "A-MQ", "A-MQU");
    }

    @Test
    public void testJoinORQueryFromNot() {
        RelationalQuery root = new JoinOrQuery(
            new RelationalNotQuery(
                new RelationalLuceneQuery("coreB", "B", new TermQuery(new Term("O", "true")))
            ),
            new RelationalLuceneQuery("coreA", "A", new TermQuery(new Term("Q", "true"))));
        KeyBits result = root.collectKeys(this.lucenes);
        LuceneResponse response = responseForResult(result, luceneA, "A");
        LuceneTest.compareHits(response, "A-Q", "A-QU", "A-MQ", "A-MQU", "A-MU");
    }

    @Test
    public void testJoinORQueryOverNotQuery() {
        RelationalQuery root = new JoinOrQuery(
            new RelationalLuceneQuery("coreB", "B", new TermQuery(new Term("O", "true"))),
            new RelationalNotQuery(
                new RelationalLuceneQuery("coreA", "A", new TermQuery(new Term("Q", "true")))
            )
        );
        KeyBits result = root.collectKeys(this.lucenes);
        LuceneResponse response = responseForResult(result, luceneA, "A");
        LuceneTest.compareHits(response, "A", "A-U", "A-M", "A-MU", "A-MQ");
    }

    @Test
    public void testJoinANDQueryOverJoinORQuery() {
        RelationalQuery root = new JoinAndQuery(
            new RelationalLuceneQuery("coreB", "B", new TermQuery(new Term("N", "true"))),
            new JoinOrQuery(
                new RelationalLuceneQuery("coreA", "A", new TermQuery(new Term("Q", "true"))),
                new RelationalLuceneQuery("coreB", "B", new TermQuery(new Term("T", "B")))
            )
        );
        KeyBits result = root.collectKeys(this.lucenes);
        LuceneResponse response = responseForResult(result, luceneA, "A");
        LuceneTest.compareHits(response, "A-MQ", "A-MQU", "A-MU");
    }

    @Test
    public void testJoinOROverJoinORQuery() {
        RelationalQuery root = new JoinOrQuery(
            new RelationalLuceneQuery("coreB", "B", new TermQuery(new Term("O", "true"))),
            new JoinOrQuery(
                new RelationalLuceneQuery("coreB", "B", new TermQuery(new Term("O", "false"))),
                new RelationalLuceneQuery("coreA", "A", new TermQuery(new Term("Q", "true")))));
        KeyBits result = root.collectKeys(this.lucenes);
        LuceneResponse response = responseForResult(result, luceneA, "A");
        LuceneTest.compareHits(response, "A-Q", "A-QU", "A-MQ", "A-MQU", "A-M", "A-MU");
    }

    @Test
    public void testJoinOROverJoinANDQuery() {
        RelationalQuery root = new JoinOrQuery(
            new RelationalLuceneQuery("coreB", "B", new TermQuery(new Term("O", "true"))),
            new JoinAndQuery(
                new RelationalLuceneQuery("coreB", "B", new TermQuery(new Term("O", "false"))),
                new RelationalLuceneQuery("coreA", "A", new TermQuery(new Term("Q", "true")))));
        KeyBits result = root.collectKeys(this.lucenes);
        LuceneResponse response = responseForResult(result, luceneA, "A");
        LuceneTest.compareHits(response, "A-MQ", "A-MQU", "A-M");
    }

    @Test
    public void testJoinOROverNotOverJoinORQuery() {
        RelationalQuery root;
        KeyBits result;
        LuceneResponse response;

        root = new JoinOrQuery(
            new RelationalLuceneQuery("coreA", "A", new TermQuery(new Term("Q", "true"))),
            new RelationalNotQuery(
                new JoinOrQuery(
                    new RelationalLuceneQuery("coreB", "B", new TermQuery(new Term("O", "true"))),
                    new RelationalLuceneQuery("coreA", "A", new TermQuery(new Term("M", "true")))
                )
            )
        );
        result = root.collectKeys(this.lucenes);
        response = responseForResult(result, luceneA, "A");
        LuceneTest.compareHits(response, "A", "A-U", "A-Q", "A-QU", "A-MQ", "A-MQU");
    }

    @Test
    public void testJoinOROverNotOverJoinORQueryLhs() {
        RelationalQuery root;
        KeyBits result;
        LuceneResponse response;

        root =
            new JoinOrQuery(
                new RelationalNotQuery(
                    new JoinOrQuery(
                        new RelationalLuceneQuery("coreB", "B", new TermQuery(new Term("O", "true"))),
                        new RelationalLuceneQuery("coreA", "A", new TermQuery(new Term("Q", "true")))
                    )  // A-Q, A-QU, A-M, A-MQ, A-MQU
                ),  // A, A-U, A-MU
                new RelationalLuceneQuery("coreA", "A", new TermQuery(new Term("M", "true")))
            ); // A, A-U, A-M, A-MU, A-MQ, A-MQU
        result = root.collectKeys(this.lucenes);
        response = responseForResult(result, luceneA, "A");
        LuceneTest.compareHits(response, "A", "A-U", "A-M", "A-MU", "A-MQ", "A-MQU");
    }

    @Test
    public void testJoinOROverNotOverJoinANDQuery() {
        RelationalQuery root;
        KeyBits result;
        LuceneResponse response;

        root = new JoinOrQuery(
            new RelationalLuceneQuery("coreA", "A", new TermQuery(new Term("Q", "true"))),
            new RelationalNotQuery(
                new JoinAndQuery(
                    new RelationalLuceneQuery("coreB", "B", new TermQuery(new Term("O", "true"))),
                    new RelationalLuceneQuery("coreA", "A", new TermQuery(new Term("M", "true")))
                )
            )
        );
        result = root.collectKeys(this.lucenes);
        response = responseForResult(result, luceneA, "A");
        LuceneTest.compareHits(response, "A", "A-U", "A-Q", "A-QU", "A-MU", "A-MQ", "A-MQU");

        root = new JoinOrQuery(
            new RelationalNotQuery(
                new JoinAndQuery(
                    new RelationalLuceneQuery("coreB", "B", new TermQuery(new Term("O", "true"))),
                    new RelationalLuceneQuery("coreA", "A", new TermQuery(new Term("M", "true")))
                )
            ),
            new RelationalLuceneQuery("coreA", "A", new TermQuery(new Term("Q", "true")))
        );
        result = root.collectKeys(this.lucenes);
        response = responseForResult(result, luceneA, "A");
        LuceneTest.compareHits(response, "A", "A-U", "A-Q", "A-QU", "A-MU", "A-MQ", "A-MQU");

    }

    @Test
    public void testJoinANDOverNotQueryOverJoinORQuery() {
        RelationalQuery root;
        KeyBits result;
        LuceneResponse response;

        root = new JoinAndQuery(
            new RelationalLuceneQuery("coreA", "A", new TermQuery(new Term("Q", "true"))),
            new RelationalNotQuery(
                new JoinOrQuery(
                    new RelationalLuceneQuery("coreB", "B", new TermQuery(new Term("O", "true"))),
                    new RelationalLuceneQuery("coreA", "A", new TermQuery(new Term("M", "true")))
                )
            )
        );
        result = root.collectKeys(this.lucenes);
        response = responseForResult(result, luceneA, "A");
        LuceneTest.compareHits(response, "A-Q", "A-QU");
    }

    @Test
    public void testJoinOROverJoinANDOverJoinORQuery() {
        RelationalQuery root;
        KeyBits result;
        LuceneResponse response;

        root = new JoinOrQuery(
            new RelationalLuceneQuery("coreA", "A", new TermQuery(new Term("U", "true"))),
            new JoinAndQuery(
                new RelationalLuceneQuery("coreA", "A", new TermQuery(new Term("Q", "true"))),
                new JoinOrQuery(
                    new RelationalLuceneQuery("coreB", "B", new TermQuery(new Term("O", "true"))),
                    new RelationalLuceneQuery("coreA", "A", new TermQuery(new Term("M", "true")))
                )
            )
        );
        result = root.collectKeys(this.lucenes);
        response = responseForResult(result, luceneA, "A");
        LuceneTest.compareHits(response, "A-U", "A-QU", "A-MU", "A-MQ", "A-MQU");
    }

    @Test
    public void testJoinOROverJoinANDOverNotOverJoinORQuery() {
        RelationalQuery root;
        KeyBits result;
        LuceneResponse response;

        root = new JoinOrQuery(
            new RelationalLuceneQuery("coreA", "A", new TermQuery(new Term("U", "true"))),
            new JoinAndQuery(
                new RelationalLuceneQuery("coreA", "A", new TermQuery(new Term("Q", "true"))),
                new RelationalNotQuery(
                    new JoinOrQuery(
                        new RelationalLuceneQuery("coreB", "B", new TermQuery(new Term("O", "true"))),
                        new RelationalLuceneQuery("coreA", "A", new TermQuery(new Term("M", "true")))
                    )
                )
            )
        );
        result = root.collectKeys(this.lucenes);
        response = responseForResult(result, luceneA, "A");
        LuceneTest.compareHits(response, "A-U", "A-Q", "A-QU", "A-MU", "A-MQU");
    }

    @Test
    public void testJoinANDOverJoinOROverJoinANDOverJoinORQuery() {
        RelationalQuery root;
        KeyBits result;
        LuceneResponse response;

        root = new JoinAndQuery(
            new JoinOrQuery(
                new RelationalLuceneQuery("coreA", "A", new TermQuery(new Term("S", "4"))),
                new RelationalLuceneQuery("coreA", "A", new TermQuery(new Term("S", "7")))
            ),
            new JoinOrQuery(
                new RelationalLuceneQuery("coreA", "A", new TermQuery(new Term("U", "true"))),
                new JoinAndQuery(
                    new RelationalLuceneQuery("coreA", "A", new TermQuery(new Term("Q", "true"))),
                    new JoinOrQuery(
                        new RelationalLuceneQuery("coreB", "B", new TermQuery(new Term("O", "true"))),
                        new RelationalLuceneQuery("coreA", "A", new TermQuery(new Term("M", "true")))
                    )
                )
            )
        );
        result = root.collectKeys(this.lucenes);
        response = responseForResult(result, luceneA, "A");
        LuceneTest.compareHits(response, "A-QU", "A-MQ");
    }

    @Test
    public void testJoinOROverJoinOROverJoinANDOverJoinORQuery() {
        RelationalQuery root;
        KeyBits result;
        LuceneResponse response;

        root = new JoinOrQuery(
            new RelationalLuceneQuery("coreA", "A", new TermQuery(new Term("S", "1"))),
            new JoinOrQuery(
                new RelationalLuceneQuery("coreA", "A", new TermQuery(new Term("U", "true"))),
                new JoinAndQuery(
                    new RelationalLuceneQuery("coreA", "A", new TermQuery(new Term("Q", "true"))),
                    new JoinOrQuery(
                        new RelationalLuceneQuery("coreB", "B", new TermQuery(new Term("O", "true"))),
                        new RelationalLuceneQuery("coreA", "A", new TermQuery(new Term("M", "true")))
                    )
                )
            )
        );
        result = root.collectKeys(this.lucenes);
        response = responseForResult(result, luceneA, "A");
        LuceneTest.compareHits(response, "A", "A-U", "A-QU", "A-MU", "A-MQ", "A-MQU");
    }

    @Test
    public void testJoinOROverJoinANDOverNotOverJoinANDQuery() {
        RelationalQuery root;
        KeyBits result;
        LuceneResponse response;

        root = new JoinOrQuery(
            new RelationalLuceneQuery("coreA", "A", new TermQuery(new Term("U", "true"))),
            new JoinAndQuery(
                new RelationalLuceneQuery("coreA", "A", new TermQuery(new Term("Q", "true"))),
                new RelationalNotQuery(
                    new JoinAndQuery(
                        new RelationalLuceneQuery("coreB", "B", new TermQuery(new Term("O", "true"))),
                        new RelationalLuceneQuery("coreA", "A", new TermQuery(new Term("M", "true")))
                    )
                )
            )
        );
        result = root.collectKeys(this.lucenes);
        response = responseForResult(result, luceneA, "A");
        LuceneTest.compareHits(response, "A-U", "A-Q", "A-QU", "A-MU", "A-MQU");
    }

    @Test
    public void testJoinANDOverJoinOROverNotOverJoinORQuery() {
        RelationalQuery root;
        KeyBits result;
        LuceneResponse response;

        root = new JoinAndQuery(
            new RelationalLuceneQuery("coreA", "A", new TermQuery(new Term("U", "true"))),
            new JoinOrQuery(
                new RelationalLuceneQuery("coreA", "A", new TermQuery(new Term("Q", "true"))),
                new RelationalNotQuery(
                    new JoinOrQuery(
                        new RelationalLuceneQuery("coreB", "B", new TermQuery(new Term("O", "true"))),
                        new RelationalLuceneQuery("coreA", "A", new TermQuery(new Term("M", "true")))
                    )
                )
            )
        );
        result = root.collectKeys(this.lucenes);
        response = responseForResult(result, luceneA, "A");
        LuceneTest.compareHits(response, "A-U", "A-QU", "A-MQU");
    }

    @Test
    public void testNotOverJoinOrOverNotOverJoinOrQuery() {
        RelationalQuery root;
        KeyBits result;
        LuceneResponse response;

        root = new RelationalNotQuery(
            new JoinOrQuery(
                new RelationalNotQuery(
                    new JoinOrQuery(
                        new RelationalLuceneQuery("coreB", "B", new TermQuery(new Term("O", "true"))),
                        new RelationalLuceneQuery("coreA", "A", new TermQuery(new Term("Q", "true")))
                    )  // A-Q, A-QU, A-M, A-MQ, A-MQU
                ),  // A, A-U, A-MU
                new RelationalLuceneQuery("coreA", "A", new TermQuery(new Term("M", "true")))
            ) // A, A-U, A-M, A-MU, A-MQ, A-MQU
        ); // A-Q, A-QU
        result = root.collectKeys(this.lucenes);
        response = responseForResult(result, luceneA, "A");
        LuceneTest.compareHits(response, "A-Q", "A-QU");
    }

    private LuceneResponse responseForResult(KeyBits result, Lucene lucene, String keyName) {
        try {
            Query keyFilter = new KeyFilter(result.bitset, keyName);
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

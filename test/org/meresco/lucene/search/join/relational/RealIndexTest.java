package org.meresco.lucene.search.join.relational;

import java.io.File;
import java.util.Arrays;
import java.util.Date;

import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.junit.Before;
import org.junit.Test;
import org.meresco.lucene.ComposedQuery;
import org.meresco.lucene.JsonQueryConverter;
import org.meresco.lucene.Lucene;
import org.meresco.lucene.LuceneResponse;
import org.meresco.lucene.LuceneSettings;
import org.meresco.lucene.MultiLucene;
import org.meresco.lucene.SeecrTestCase;
import org.meresco.lucene.search.TermFrequencySimilarity;


public class RealIndexTest extends SeecrTestCase {
    MultiLucene multiLucene;
    Lucene luceneSummary;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        LuceneSettings settingsSummary = new LuceneSettings();
        FacetsConfig facetsConfig = settingsSummary.facetsConfig;
        String dimName = "untokenized.ggc:kmc0500par103.uri.byOntology";
        facetsConfig.setHierarchical(dimName, true);
        facetsConfig.setMultiValued(dimName, true);
        facetsConfig.setIndexFieldName(dimName, "$facets_ggc:kmc0500par103.uri.byOntology");

        LuceneSettings settingsHolding = new LuceneSettings();
        LuceneSettings settingsRank = new LuceneSettings();
        settingsRank.similarity = new TermFrequencySimilarity();
        this.luceneSummary = new Lucene("summary", new File("/data/NBC-340-lucene/lucene-summary").toPath(), settingsSummary);
        Lucene luceneHolding = new Lucene("holding", new File("/data/NBC-340-lucene/lucene-holding").toPath(), settingsHolding);
        Lucene luceneRank = new Lucene("rank", new File("/data/NBC-340-lucene/lucene-rank").toPath(), settingsRank);
        this.multiLucene = new MultiLucene(Arrays.asList(luceneSummary, luceneHolding, luceneRank));
    }

    //@Ignore("requires live index")
    @Test
    public void testQuery() throws Throwable {
        Query warmupFilter = new WrappedRelationalQuery(
            new JoinANDQuery(
                new LuceneQuery("summary", "__key__.lh:item.uri",
                    new TermQuery(new Term("__all__", "vliegtuig"))),
                new LuceneQuery("holding", "__key__.lh:item.uri",
                    new TermQuery(new Term("untokenized.lh:holder.uri", "info:isil:NL-0800070000")))));
        ComposedQuery composedQuery = new ComposedQuery("summary", new MatchAllDocsQuery());
        composedQuery.addMatch("summary", "holding", "__key__.lh:item.uri", "__key__.lh:item.uri");

        Date t0 = new Date();
        LuceneResponse response = this.multiLucene.executeComposedQuery(composedQuery);
        Date t1 = new Date();
        System.out.println("MatchAllDocs: " + response.total + " results: " + response.hits);
        System.out.println("MatchAllDocs query took: " + (t1.getTime() - t0.getTime()));
        System.out.println("\n");

        composedQuery.relationalFilter = warmupFilter;
        t0 = new Date();
        response = this.multiLucene.executeComposedQuery(composedQuery);
        t1 = new Date();
        System.out.println("" + response.total + " results: " + response.hits);
        System.out.println("warmup query took: " + (t1.getTime() - t0.getTime()));
        System.out.println("\n");

        WrappedRelationalQuery fietsBnlFilterQuery = new WrappedRelationalQuery(
            new JoinANDQuery(
                new LuceneQuery("summary", "__key__.lh:item.uri",
                    new TermQuery(new Term("__all__", "fiets"))),
                new LuceneQuery("holding", "__key__.lh:item.uri",
                    new TermQuery(new Term("untokenized.lh:holder.uri", "info:isil:NL-0773600000")))));
        composedQuery.relationalFilter = fietsBnlFilterQuery;

        JsonQueryConverter summaryJsonQueryConverter = this.luceneSummary.getQueryConverter();
        Term onvolledigDrilldownTerm = summaryJsonQueryConverter.createDrilldownTerm(
                "untokenized.ggc:kmc0500par103.uri.byOntology",
                "http://data.bibliotheek.nl/ns/nbc/Oclc/kmc0500par103#", "http://data.bibliotheek.nl/ns/nbc/Oclc/kmc0500par103#y");
        Term onvolledigInKaderNbcDrilldownTerm = summaryJsonQueryConverter.createDrilldownTerm(
                "untokenized.ggc:kmc0500par103.uri.byOntology",
                "http://data.bibliotheek.nl/ns/nbc/Oclc/kmc0500par103#", "http://data.bibliotheek.nl/ns/nbc/Oclc/kmc0500par103#p");
        Term acquisitieDrilldownTerm = summaryJsonQueryConverter.createDrilldownTerm(
                "untokenized.ggc:kmc0500par103.uri.byOntology",
                "http://data.bibliotheek.nl/ns/nbc/Oclc/kmc0500par103#", "http://data.bibliotheek.nl/ns/nbc/Oclc/kmc0500par103#a");

        BooleanQuery.Builder unwantedPar103Query = new BooleanQuery.Builder();
        unwantedPar103Query.add(new TermQuery(onvolledigDrilldownTerm), Occur.SHOULD);
        unwantedPar103Query.add(new TermQuery(onvolledigInKaderNbcDrilldownTerm), Occur.SHOULD);
        unwantedPar103Query.add(new TermQuery(acquisitieDrilldownTerm), Occur.SHOULD);

        composedQuery.relationalFilter = new WrappedRelationalQuery(
            new NotQuery(
                new JoinANDQuery(
                    new LuceneQuery("summary", "__key__.lh:item.uri", unwantedPar103Query.build()),
                    new LuceneQuery("holding", "__key__.lh:item.uri",
                        new TermQuery(new Term("untokenized.lh:holder.uri", "info:isil:NL-0100030000"))))));

        t0 = new Date();
        response = this.multiLucene.executeComposedQuery(composedQuery);
        t1 = new Date();
        response = this.multiLucene.executeComposedQuery(composedQuery);
        System.out.println("" + response.total + " results: " + response.hits);
        System.out.println("new filter took: " + (t1.getTime() - t0.getTime()));

        int N = 10;
        long t = 0;
        for (int i = 0; i < N; i++) {
            t0 = new Date();
            response = this.multiLucene.executeComposedQuery(composedQuery);
            t += (new Date().getTime() - t0.getTime());
        }
        System.out.println("same query+filter once again took on average: " + (t / N));

        composedQuery.setCoreQuery("summary", new TermQuery(new Term("__all__", "foetsie")));
        t0 = new Date();
        response = this.multiLucene.executeComposedQuery(composedQuery);
        t1 = new Date();
        System.out.println("\nsame filter + new query took: " + (t1.getTime() - t0.getTime()));
        System.out.println("" + response.total + " results: " + response.hits);

    }
}

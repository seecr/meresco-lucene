/* begin license *
 *
 * "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
 *
 * Copyright (C) 2015-2016 Koninklijke Bibliotheek (KB) http://www.kb.nl
 * Copyright (C) 2015-2016, 2019 Seecr (Seek You Too B.V.) https://seecr.nl
 * Copyright (C) 2016 Stichting Kennisnet http://www.kennisnet.nl
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

package org.meresco.lucene;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import javax.json.JsonArray;
import javax.json.JsonNumber;
import javax.json.JsonObject;
import javax.json.JsonString;
import javax.json.JsonValue;

import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.spell.SuggestWord;
import org.junit.Test;
import org.meresco.lucene.LuceneResponse.ClusterHit;
import org.meresco.lucene.LuceneResponse.DedupHit;
import org.meresco.lucene.LuceneResponse.DrilldownData;
import org.meresco.lucene.LuceneResponse.Hit;
import org.meresco.lucene.search.MerescoCluster;
import org.meresco.lucene.search.MerescoCluster.DocScore;
import org.meresco.lucene.search.MerescoCluster.TermScore;


public class LuceneResponseToJsonTest {
    @Test
    public void test() {
        LuceneResponse response = new LuceneResponse(2);
        response.addHit(new LuceneResponse.Hit("id1", 0.1f));
        response.addHit(new LuceneResponse.Hit("id2", 0.2f));
        LuceneResponse.DrilldownData dd = new DrilldownData("field");
        List<DrilldownData.Term> terms = new ArrayList<>();
        terms.add(new DrilldownData.Term("value1", 1));
        DrilldownData.Term t = new DrilldownData.Term("value2", 5);
        t.subTerms = new ArrayList<>();
        t.subTerms.add(new DrilldownData.Term("subValue2", 1));
        terms.add(t);
        dd.terms = terms;
        response.drilldownData = new ArrayList<>();
        response.drilldownData.add(dd);

        response.times.put("facetTime", 12L);
        SuggestWord sug1 = new SuggestWord();
        sug1.string = "value";
        response.suggestions.put("valeu", new SuggestWord[] {sug1});

        JsonObject jsonResponse = response.toJson();
        assertEquals(2, jsonResponse.getInt("total"));
        assertEquals(0, jsonResponse.getInt("queryTime"));

        JsonArray hits = jsonResponse.getJsonArray("hits");
        assertEquals(2, hits.size());
        assertEquals("id1", hits.getJsonObject(0).getString("id"));
        assertEquals(0.1, hits.getJsonObject(0).getJsonNumber("score").doubleValue(), 0.0001);
        assertEquals("id2", hits.getJsonObject(1).getString("id"));
        assertEquals(0.2, hits.getJsonObject(1).getJsonNumber("score").doubleValue(), 0.0001);

        JsonArray ddData = jsonResponse.getJsonArray("drilldownData");
        assertEquals(1, ddData.size());
        assertEquals("field", ddData.getJsonObject(0).getString("fieldname"));
        assertEquals(0, ddData.getJsonObject(0).getJsonArray("path").size());
        JsonArray ddTerms = ddData.getJsonObject(0).getJsonArray("terms");
        assertEquals("value1", ddTerms.getJsonObject(0).getString("term"));
        assertEquals(1, ddTerms.getJsonObject(0).getInt("count"));
        JsonArray subterms = ddTerms.getJsonObject(0).getJsonArray("subterms");
        assertEquals(null, subterms);
        JsonArray subterms2 = ddTerms.getJsonObject(1).getJsonArray("subterms");
        assertEquals(1, subterms2.size());
        assertEquals("subValue2", subterms2.getJsonObject(0).getString("term"));
        assertEquals(1, subterms2.getJsonObject(0).getInt("count"));

        JsonObject times = jsonResponse.getJsonObject("times");
        assertEquals(1, times.size());
        assertEquals(12L, times.getJsonNumber("facetTime").longValue());

        JsonObject suggestions = jsonResponse.getJsonObject("suggestions");
        assertEquals(1, suggestions.size());
        JsonArray suggestionsValeu = suggestions.getJsonArray("valeu");
        assertEquals(1, suggestionsValeu.size());
        assertEquals("value", suggestionsValeu.getString(0));
    }

    @Test
    public void testHierarchicalDrilldown() {
        LuceneResponse response = new LuceneResponse(2);
        LuceneResponse.DrilldownData dd = new DrilldownData("field");
        List<DrilldownData.Term> terms = new ArrayList<>();
        terms.add(new DrilldownData.Term("value1", 1));
        dd.path = new String[] {"subpath"};
        dd.terms = terms;
        response.drilldownData = new ArrayList<>();
        response.drilldownData.add(dd);
        JsonObject jsonResponse = response.toJson();
        JsonArray ddData = jsonResponse.getJsonArray("drilldownData");
        assertEquals(1, ddData.size());
        assertEquals("field", ddData.getJsonObject(0).getString("fieldname"));
        assertEquals("subpath", ddData.getJsonObject(0).getJsonArray("path").getString(0));
        JsonArray ddTerms = ddData.getJsonObject(0).getJsonArray("terms");
        assertEquals("value1", ddTerms.getJsonObject(0).getString("term"));
        assertEquals(1, ddTerms.getJsonObject(0).getInt("count"));
    }

    @Test
    public void testDedup() {
        LuceneResponse response = new LuceneResponse(2);
        response.totalWithDuplicates = 5L;
        DedupHit hit1 = new DedupHit("id1", 0.1f);
        hit1.duplicateField = "__key__";
        hit1.duplicateCount = 2;
        response.addHit(hit1);
        DedupHit hit2 = new DedupHit("id2", 0.2f);
        hit2.duplicateField = "__key__";
        hit2.duplicateCount = 5;
        response.addHit(hit2);

        JsonObject json = response.toJson();
        assertEquals(5, json.getInt("totalWithDuplicates"));
        JsonArray hits = json.getJsonArray("hits");
        assertEquals(2, hits.size());
        JsonObject duplicateCount = hits.getJsonObject(0).getJsonObject("duplicateCount");
        assertEquals(2, duplicateCount.getInt("__key__"));
        duplicateCount = hits.getJsonObject(1).getJsonObject("duplicateCount");
        assertEquals(5, duplicateCount.getInt("__key__"));
    }

    @Test
    public void testClustering() {
        LuceneResponse response = new LuceneResponse(2);
        ClusterHit hit1 = new ClusterHit("id1", 0.1f);
        hit1.topDocs = new MerescoCluster.DocScore[] { new DocScore(0, 0.1f), new DocScore(1, 0.2f) };
        hit1.topDocs[0].identifier = "id1";
        hit1.topDocs[1].identifier = "id2";
        hit1.topTerms = new MerescoCluster.TermScore[] { new TermScore("term1", 0), new TermScore("term2", 1) };
        response.addHit(hit1);

        JsonArray hits = response.toJson().getJsonArray("hits");
        assertEquals(1, hits.size());
        JsonObject duplicates = hits.getJsonObject(0).getJsonObject("duplicates");
        JsonArray topDocs = duplicates.getJsonArray("topDocs");
        assertEquals(2, topDocs.size());
        assertEquals("id1", topDocs.getJsonObject(0).getString("id"));
        assertEquals(0.1, topDocs.getJsonObject(0).getJsonNumber("score").doubleValue(), 0.000001);
        assertEquals("id2", topDocs.getJsonObject(1).getString("id"));
        assertEquals(0.2, topDocs.getJsonObject(1).getJsonNumber("score").doubleValue(), 0.000001);

        JsonArray topTerms = duplicates.getJsonArray("topTerms");
        assertEquals(2, topTerms.size());
        assertEquals("term1", topTerms.getJsonObject(0).getString("term"));
        assertEquals(0, topTerms.getJsonObject(0).getJsonNumber("score").doubleValue(), 0);
        assertEquals("term2", topTerms.getJsonObject(1).getString("term"));
        assertEquals(1, topTerms.getJsonObject(1).getJsonNumber("score").doubleValue(), 0);
    }

    @Test
    public void testHitWithoutId() {
        LuceneResponse response = new LuceneResponse(1);
        response.addHit(new LuceneResponse.Hit(null, 0.1f));

        JsonObject json = response.toJson();
        assertEquals(JsonValue.NULL, json.getJsonArray("hits").getJsonObject(0).get("id"));
    }

    @Test
    public void testStoredFields() {
        LuceneResponse response = new LuceneResponse(1);
        Hit hit = new Hit("id:1", 1);
        hit.fields.add(new IndexableField[] {new StringField("aField", "aValue", Store.YES)});
        hit.fields.add(new IndexableField[] {new StoredField("intField", 10)});
        response.addHit(hit);

        JsonObject json = response.toJson();
        assertEquals("id:1", json.getJsonArray("hits").getJsonObject(0).getString("id"));
        assertEquals("aValue", ((JsonString) json.getJsonArray("hits").getJsonObject(0).getJsonArray("aField").get(0)).getString());
        assertEquals(10, ((JsonNumber) json.getJsonArray("hits").getJsonObject(0).getJsonArray("intField").get(0)).intValue());
    }
}

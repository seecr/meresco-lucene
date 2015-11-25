/* begin license *
 *
 * "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
 *
 * Copyright (C) 2015 Koninklijke Bibliotheek (KB) http://www.kb.nl
 * Copyright (C) 2015 Seecr (Seek You Too B.V.) http://seecr.nl
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

import javax.json.JsonArray;
import javax.json.JsonObject;

import org.apache.lucene.facet.LabelAndValue;
import org.junit.Test;
import org.meresco.lucene.LuceneResponse.DrilldownData;

public class LuceneResponseToJson {

    @Test
    public void test() {
        LuceneResponse response = new LuceneResponse(2);
        response.addHit("id1", 0.1f);
        response.addHit("id2", 0.2f);
        LuceneResponse.DrilldownData dd = new DrilldownData("field");
        dd.addTerm(new LabelAndValue("value1", 1));
        dd.addTerm(new LabelAndValue("value2", 5));
        response.drilldownData = new ArrayList<LuceneResponse.DrilldownData>();
        response.drilldownData.add(dd);
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
        assertEquals("value1", ddData.getJsonObject(0).getJsonArray("terms").getJsonObject(0).getString("term"));
        assertEquals(1, ddData.getJsonObject(0).getJsonArray("terms").getJsonObject(0).getInt("count"));
    }

}

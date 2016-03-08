/* begin license *
 *
 * "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
 *
 * Copyright (C) 2015 Koninklijke Bibliotheek (KB) http://www.kb.nl
 * Copyright (C) 2015-2016 Seecr (Seek You Too B.V.) http://seecr.nl
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

import java.util.List;

import javax.json.Json;
import javax.json.JsonObject;

import org.junit.Test;
import org.meresco.lucene.ClusterConfig.ClusterField;


public class ClusterConfigTest {
    @Test
    public void testClusterConfigParseFromJsonObject() throws Exception {
        JsonObject json = Json.createObjectBuilder()
            .add("clusteringEps", 0.3)
            .add("clusteringMinPoints", 3)
           	.add("clusterMoreRecords", 200)
           	.add("fields", Json.createObjectBuilder()
                .add("dcterms:title", Json.createObjectBuilder()
           					.add("fieldname", "dcterms:title")
           					.add("filterValue", "a")
           					.add("weight", 0.3))
           			.add("dcterms:creator", Json.createObjectBuilder()
           					.add("fieldname", "dcterms:creator")
           					.add("filterValue", "b")
           					.add("weight", 0.7)))
            .build();
        ClusterConfig clusterConfig = ClusterConfig.parseFromJsonObject(json);
        assertEquals(0.3, clusterConfig.clusteringEps, 0.02);
    	  assertEquals(3, clusterConfig.clusteringMinPoints);
    	  assertEquals(200, clusterConfig.clusterMoreRecords);
        List<ClusterField> clusterFields = clusterConfig.clusterFields;
        assertEquals(2, clusterFields.size());
        ClusterField field = clusterFields.get(0);
        assertEquals("dcterms:title", field.fieldname);
        assertEquals("a", field.filterValue);
        assertEquals(0.3, field.weight, 0.02);
        field = clusterFields.get(1);
        assertEquals("dcterms:creator", field.fieldname);
        assertEquals("b", field.filterValue);
        assertEquals(0.7, field.weight, 0.02);
        assertEquals("ClusterConfig(clusteringEps=0.3, clusteringMinPoints=3, clusterMoreRecords=200, clusterFields=[ClusterField(fieldname=\"dcterms:title\", weight=0.3, filterValue=\"a\"), ClusterField(fieldname=\"dcterms:creator\", weight=0.7, filterValue=\"b\")])", clusterConfig.toString());
    }

    @Test
    public void testNoClusterConfigParsedFromJsonObject() throws Exception {
        JsonObject json = Json.createObjectBuilder().build();
        assertEquals(null, ClusterConfig.parseFromJsonObject(json));
    }
}

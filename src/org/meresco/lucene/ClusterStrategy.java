/* begin license *
 *
 * "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
 *
 * Copyright (C) 2016 Koninklijke Bibliotheek (KB) http://www.kb.nl
 * Copyright (C) 2016 Seecr (Seek You Too B.V.) https://seecr.nl
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

import java.util.ArrayList;
import java.util.List;

import javax.json.JsonObject;

public class ClusterStrategy {
	public double clusteringEps;
	public int clusteringMinPoints;
	public List<ClusterField> clusterFields = new ArrayList<>();

	public ClusterStrategy() {
	}

	public ClusterStrategy(double clusteringEps, int clusteringMinPoints) {
		this.clusteringEps = clusteringEps;
		this.clusteringMinPoints = clusteringMinPoints;
	}

	public ClusterStrategy addField(ClusterField field) {
		if (field.weight != 0.0) {
			this.clusterFields.add(field);
		}
		return this;
	}

	public ClusterStrategy addField(String fieldname, double weight, String filterValue) {
		return addField(new ClusterField(fieldname, weight, filterValue));
	}

	public static ClusterStrategy parseFromJsonObject(JsonObject jsonObject) {
		ClusterStrategy result = null;
		ClusterStrategy strategy = new ClusterStrategy();
        for (String key: jsonObject.keySet()) {
            switch (key) {
            case "clusteringEps":
            	strategy.clusteringEps = jsonObject.getJsonNumber(key).doubleValue();
            	result = strategy;
                break;
            case "clusteringMinPoints":
            	strategy.clusteringMinPoints = jsonObject.getInt(key);
            	result = strategy;
                break;
            case "fields":
                strategy.parseClusterFields(jsonObject.getJsonObject(key));
                result = strategy;
                break;
            }
        }
        return result;
	}

	@Override
	public String toString() {
		return "ClusterStrategy(clusteringEps=" + clusteringEps + ", clusteringMinPoints=" + clusteringMinPoints + ", clusterFields=" + clusterFields.toString() + ")";
	}

    private void parseClusterFields(JsonObject jsonClusterFields) {
        for (String key: jsonClusterFields.keySet()) {
            JsonObject clusterField = jsonClusterFields.getJsonObject(key);
            String filterValue = clusterField.getString("filterValue", null);
            addField(clusterField.getString("fieldname"), clusterField.getJsonNumber("weight").doubleValue(), filterValue);
        }
    }
}
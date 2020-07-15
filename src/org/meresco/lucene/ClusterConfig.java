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

import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonValue;


public class ClusterConfig {
	public int clusterMoreRecords = 0;
	public List<ClusterStrategy> strategies = new ArrayList<>();

	public ClusterConfig() {
	}

	public ClusterConfig(int clusterMoreRecords) {
		this.clusterMoreRecords = clusterMoreRecords;
	}

	public ClusterConfig(double clusteringEps, int minPoints, int clusterMoreRecords) {
		this(clusterMoreRecords);
		this.strategies.add(new ClusterStrategy(clusteringEps, minPoints));
	}

	public ClusterConfig addStrategy(ClusterStrategy strategy) {
		this.strategies.add(strategy);
		return this;
	}

	public static ClusterConfig parseFromJsonObject(JsonObject jsonObject) {
		ClusterConfig result = null;
		ClusterConfig clusterConfig = new ClusterConfig();
        for (String key: jsonObject.keySet()) {
            switch (key) {
            case "clusterMoreRecords":
            	clusterConfig.clusterMoreRecords  = jsonObject.getInt(key);
            	result = clusterConfig;
                break;
            case "strategies":
            	clusterConfig.strategies = parseClusterStrategies(jsonObject.getJsonArray(key));
            	result = clusterConfig;
                break;
            }
        }
        return result;
	}

	public String toString() {
		return "ClusterConfig(clusterMoreRecords=" + clusterMoreRecords + ", strategies=" + strategies.toString() + ")";
	}

    private static List<ClusterStrategy> parseClusterStrategies(JsonArray jsonClusterStrategies) {
        List<ClusterStrategy> clusterStrategies = new ArrayList<ClusterStrategy>();
        for (JsonValue jsonStrategy: jsonClusterStrategies) {
            clusterStrategies.add(ClusterStrategy.parseFromJsonObject((JsonObject) jsonStrategy));
        }
        return clusterStrategies;
    }
}

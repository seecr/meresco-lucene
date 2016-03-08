/* begin license *
 *
 * "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
 *
 * Copyright (C) 2016 Seecr (Seek You Too B.V.) http://seecr.nl
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


public class ClusterConfig {
	public double clusteringEps;
	public int clusteringMinPoints;
	public int clusterMoreRecords;
	public List<ClusterField> clusterFields = new ArrayList<>();

	private ClusterConfig() {
	}

	public ClusterConfig(double clusteringEps, int clusteringMinPoints, int clusterMoreRecords) {
		this.clusteringEps = clusteringEps;
		this.clusteringMinPoints = clusteringMinPoints;
		this.clusterMoreRecords = clusterMoreRecords;
	}

	public static ClusterConfig parseFromJsonObject(JsonObject jsonObject) {
		ClusterConfig result = null;
		ClusterConfig clusterConfig = new ClusterConfig();
        for (String key: jsonObject.keySet()) {
            switch (key) {
            case "clusteringEps":
            	clusterConfig.clusteringEps = jsonObject.getJsonNumber(key).doubleValue();
            	result = clusterConfig;
                break;
            case "clusteringMinPoints":
            	clusterConfig.clusteringMinPoints = jsonObject.getInt(key);
            	result = clusterConfig;
                break;
            case "clusterMoreRecords":
            	clusterConfig.clusterMoreRecords  = jsonObject.getInt(key);
            	result = clusterConfig;
                break;
            case "fields":
            	clusterConfig.clusterFields = parseClusterFields(jsonObject.getJsonObject(key));
            	result = clusterConfig;
                break;
            }
        }
        return result;
	}

	public String toString() {
		return "ClusterConfig(clusteringEps=" + clusteringEps + ", clusteringMinPoints=" + clusteringMinPoints + ", clusterMoreRecords=" + clusterMoreRecords + ", clusterFields=" + clusterFields.toString() + ")";
	}

    private static List<ClusterField> parseClusterFields(JsonObject jsonClusterFields) {
        List<ClusterField> clusterFields = new ArrayList<ClusterField>();
        for (String key: jsonClusterFields.keySet()) {
            JsonObject clusterField = jsonClusterFields.getJsonObject(key);
            String filterValue = clusterField.getString("filterValue", null);
            clusterFields.add(new ClusterField(clusterField.getString("fieldname"), clusterField.getJsonNumber("weight").doubleValue(), filterValue));
        }
        return clusterFields;
    }


	public static class ClusterField {
	    public String fieldname;
	    public double weight;
	    public String filterValue;

	    public ClusterField(String fieldname, double weight, String filterValue) {
	        this.fieldname = fieldname;
	        this.weight = weight;
	        this.filterValue = filterValue;
	    }

	    public String toString() {
	    	return "ClusterField(fieldname=\"" + fieldname + "\", weight=" + weight + ", filterValue=" + (filterValue == null ? "null" : "\"" + filterValue + "\"") + ")";
	    }
	}
}
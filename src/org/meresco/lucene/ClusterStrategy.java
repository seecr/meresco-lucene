package org.meresco.lucene;

import java.util.ArrayList;
import java.util.List;

import javax.json.JsonObject;

public class ClusterStrategy {
	public double clusteringEps;
	public int clusteringMinPoints;
	public List<ClusterField> clusterFields = new ArrayList<>();

	private ClusterStrategy() {
	}

	public ClusterStrategy(double clusteringEps, int clusteringMinPoints) {
		this.clusteringEps = clusteringEps;
		this.clusteringMinPoints = clusteringMinPoints;
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
            	strategy.clusterFields = parseClusterFields(jsonObject.getJsonObject(key));
            	result = strategy;
                break;
            }
        }
        return result;
	}

	public String toString() {
		return "ClusterStrategy(clusteringEps=" + clusteringEps + ", clusteringMinPoints=" + clusteringMinPoints + ", clusterFields=" + clusterFields.toString() + ")";
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
}
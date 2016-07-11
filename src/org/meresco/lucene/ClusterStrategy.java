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
package org.meresco.lucene;

public class ClusterField {
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